package main

import (
	"fmt"
	"os"
	"strings"

	mmsemver "github.com/Masterminds/semver/v3"
	blangsemver "github.com/blang/semver/v4"
	"github.com/operator-framework/operator-registry/alpha/action"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/model"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/yaml"

	v1 "fbc-filter/api/config/v1"
)

func main() {
	var (
		configFile string
		migrate    bool
		output     string
	)
	cmd := &cobra.Command{
		Use:  "fbc-filter --config <config> <catalogReference> [<flags>]",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			configData, err := os.ReadFile(configFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error reading configuration file: %v\n", err)
				os.Exit(1)
			}
			var config v1.FilterConfiguration
			if err := yaml.Unmarshal(configData, &config); err != nil {
				fmt.Fprintf(os.Stderr, "error parsing configuration file: %v\n", err)
				os.Exit(1)
			}
			if config.Kind != "FilterConfiguration" && config.APIVersion != "olm.operatorframework.io/v1" {
				fmt.Fprintf(os.Stderr, "invalid configuration file: expected kind FilterConfiguration and APIVersion olm.operatorframework.io/v1, got %s/%s\n", config.Kind, config.APIVersion)
				os.Exit(1)
			}
			r := action.Render{
				Refs:           args,
				Registry:       nil,
				AllowedRefMask: action.RefDCDir | action.RefDCImage | action.RefSqliteFile | action.RefSqliteImage,
				Migrate:        migrate,
			}
			fbc, err := r.Run(cmd.Context())
			if err != nil {
				fmt.Fprintf(os.Stderr, "error rendering input: %v\n", err)
				os.Exit(1)
			}
			if err := filterV1(fbc, config, func(format string, args ...interface{}) {
				fmt.Fprintf(os.Stderr, format+"\n", args...)
			}); err != nil {
				fmt.Fprintf(os.Stderr, "error filtering input: %v\n", err)
				os.Exit(1)
			}

			var write declcfg.WriteFunc
			switch output {
			case "yaml":
				write = declcfg.WriteYAML
			case "json":
				write = declcfg.WriteJSON
			default:
				fmt.Fprintf(os.Stderr, "invalid output format: %s\n", output)
			}
			if err := write(*fbc, os.Stdout); err != nil {
				fmt.Fprintf(os.Stderr, "error writing output: %v\n", err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().BoolVar(&migrate, "migrate", false, "Migrate the input to the latest version")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format")
	cmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to the filter configuration file")
	cmd.MarkFlagRequired("config")
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error executing command: %v\n", err)
		os.Exit(1)
	}
}

type logFunc func(string, ...interface{})

func filterV1(fbc *declcfg.DeclarativeConfig, configuration v1.FilterConfiguration, warnf logFunc) error {
	m, err := declcfg.ConvertToModel(*fbc)
	if err != nil {
		return err
	}

	// first filter out packages
	filterPackages(m, configuration.Packages, warnf)

	// then filter out channels
	for _, p := range configuration.Packages {
		pkgModel, ok := m[p.Name]
		if !ok {
			warnf("package %q not found in catalog", p.Name)
			continue
		}

		if err := filterChannels(pkgModel, p, warnf); err != nil {
			return fmt.Errorf("could not filter channels in package %q: %v", p.Name, err)
		}

		// for the remaining channels, filter out bundles that don't match the version range
		for _, c := range p.Channels {
			ch, ok := pkgModel.Channels[c.Name]
			if !ok {
				warnf("channel %q not found in package %q", c.Name, p.Name)
				continue
			}
			if c.VersionRange != "" {
				if err := filterBundles(ch, c, warnf); err != nil {
					return fmt.Errorf("could not filter bundles in package %q: %v", c.Name, err)
				}
			}
		}
	}
	if err := m.Validate(); err != nil {
		return fmt.Errorf("filtered model is invalid: %v", err)
	}
	*fbc = declcfg.ConvertFromModel(m)
	return nil
}

func filterPackages(m model.Model, packageConfigs []v1.Package, warnf logFunc) {
	// first filter out packages
	packages := sets.New[string]()
	for _, p := range packageConfigs {
		packages.Insert(p.Name)
	}
	for _, pkg := range m {
		if !packages.Has(pkg.Name) {
			delete(m, pkg.Name)
		}
	}
}

func filterChannels(p *model.Package, pkgConfig v1.Package, warnf logFunc) error {
	if len(pkgConfig.Channels) > 0 {
		channels := sets.New[string]()
		for _, c := range pkgConfig.Channels {
			channels.Insert(c.Name)
		}
		for _, ch := range p.Channels {
			if !channels.Has(ch.Name) {
				delete(p.Channels, ch.Name)
			}
		}
	}
	if err := setDefaultChannel(p, pkgConfig, warnf); err != nil {
		return fmt.Errorf("invalid default channel filter configuration: %v", err)
	}
	return nil
}

func setDefaultChannel(p *model.Package, pkgConfig v1.Package, warnf logFunc) error {
	// lots of complexity here. let's enumerate the cases
	// 1. when default channel is set in the package config
	//    a. if the configured channel exists after filtering, update model's default channel
	//    b. if the configured channel does not exist after filtering
	//       i. does the original model's default channel exist after filtering?
	//          - if yes: warn: specified default channel override does not exist, keeping original default channel from catalog
	//          - if no: specified default channel override does not exist, and original default channel does not exist
	// 2. when the default channel is not set in the package config
	//    a. if the original model's default channel does not exist after filtering, error: "default channel must be configured"

	_, defaultChannelStillExists := p.Channels[p.DefaultChannel.Name]
	if pkgConfig.DefaultChannel != "" {
		if configDefaultChannel, ok := p.Channels[pkgConfig.DefaultChannel]; ok {
			p.DefaultChannel = configDefaultChannel
		} else if defaultChannelStillExists {
			warnf("specified default channel override %q does not exist, keeping original default channel from catalog", pkgConfig.DefaultChannel)
		} else {
			return fmt.Errorf("specified default channel override %q does not exist, and original default channel %q does not exist", pkgConfig.DefaultChannel, p.DefaultChannel.Name)
		}
		return nil
	}
	if !defaultChannelStillExists {
		return fmt.Errorf("the default channel %q was filtered out, a new default channel must be configured in the FilterConfiguration for this package", p.DefaultChannel.Name)
	}
	return nil
}

func filterBundles(ch *model.Channel, channelConfig v1.Channel, warnf logFunc) error {
	// we need to keep a single coherent channel head, which might mean including one extra bundle that falls outside
	// the minVersion/maxVersion range. this case happens when a bundle on the replaces chain:
	//   1. is not in the minVersion/maxVersion range
	//   2. contains a bundle in its replaces chain that is in the minVersion/maxVersion range
	//   3. contains a bundle in its skips list that is in the minVersion/maxVersion range
	// if this happens, we will emit a warning and include the bundle as the new channel head.

	cur, err := ch.Head()
	if err != nil {
		return fmt.Errorf("error getting head of channel %q: %v", ch.Name, err)
	}

	versionRange, err := mmsemver.NewConstraint(channelConfig.VersionRange)
	if err != nil {
		return fmt.Errorf("invalid version range %q for channel %q: %v", channelConfig.VersionRange, ch.Name, err)
	}

	var head *model.Bundle
	for cur != nil && head == nil {
		curVersion := blangToMM(cur.Version)
		if versionRange.Check(curVersion) {
			head = cur
			break
		}
		for _, skip := range cur.Skips {
			skipBundle, ok := ch.Bundles[skip]
			if !ok {
				continue
			}
			skipVersion := blangToMM(skipBundle.Version)
			if versionRange.Check(skipVersion) {
				head = cur
				break
			}
		}
		cur = ch.Bundles[cur.Replaces]
	}
	var tail *model.Bundle
	for cur != nil {
		if !isOrContainsBundleInVersionRange(cur, versionRange, ch) {
			tail = cur
			break
		}
		cur = ch.Bundles[cur.Replaces]
	}

	// we how have head and tail, let's traverse head to tail and build a list of bundles to keep
	// warn if anything in the replaces chain is not in the version range
	bundles := map[string]*model.Bundle{}
	for cur = head; cur != tail; cur = ch.Bundles[cur.Replaces] {
		curVersion := blangToMM(cur.Version)
		if !versionRange.Check(curVersion) {
			warnf("including bundle %q with version %q in channel %q for package %q: it falls outside the specified range of %q but is required to ensure inclusion of all bundles in the range", cur.Name, curVersion.String(), ch.Name, ch.Package.Name, channelConfig.VersionRange)
		}
		bundles[cur.Name] = cur
		for _, skip := range cur.Skips {
			if skipBundle, ok := ch.Bundles[skip]; ok {
				skipVersion := blangToMM(skipBundle.Version)
				if versionRange.Check(skipVersion) {
					bundles[skipBundle.Name] = skipBundle
				}
			}
		}
	}
	if len(bundles) == 0 {
		return fmt.Errorf("invalid filter configuration: no bundles in channel %q for package %q matched the version range %q", ch.Name, ch.Package.Name, channelConfig.VersionRange)
	}
	ch.Bundles = bundles
	return nil
}

func isOrContainsBundleInVersionRange(b *model.Bundle, versionRange *mmsemver.Constraints, ch *model.Channel) bool {
	bVersion := blangToMM(b.Version)
	if versionRange.Check(bVersion) {
		return true
	}
	for _, skip := range b.Skips {
		if skipBundle, ok := ch.Bundles[skip]; ok {
			skipVersion := blangToMM(skipBundle.Version)
			if versionRange.Check(skipVersion) {
				return true
			}
		}
	}
	if replacesBundle, ok := ch.Bundles[b.Replaces]; ok {
		return isOrContainsBundleInVersionRange(replacesBundle, versionRange, ch)
	}
	return false
}

func blangToMM(in blangsemver.Version) *mmsemver.Version {
	pres := make([]string, len(in.Pre))
	for i, p := range in.Pre {
		pres[i] = p.String()
	}
	return mmsemver.New(
		in.Major,
		in.Minor,
		in.Patch,
		strings.Join(pres, "."),
		strings.Join(in.Build, "."),
	)
}
