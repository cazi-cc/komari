package tasks

import (
	"encoding/json"
	"math"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/internal/config"
)

const tcpQualityScoreModelVersion = 3

type tcpQualityScoreConfig struct {
	ModelVersion                 int     `json:"tcpQualityScoreModelVersion"`
	OverallICMPWeight            float64 `json:"tcpOverallICMPWeight"`
	OverallStandardWeight        float64 `json:"tcpOverallStandardWeight"`
	OverallLargeWeight           float64 `json:"tcpOverallLargeWeight"`
	StandardLossWeight           float64 `json:"tcpStandardLossWeight"`
	StandardP50Weight            float64 `json:"tcpStandardP50Weight"`
	StandardP95Weight            float64 `json:"tcpStandardP95Weight"`
	StandardCoverageWeight       float64 `json:"tcpStandardCoverageWeight"`
	LargeLossWeight              float64 `json:"tcpLargeLossWeight"`
	LargeExtraLossWeight         float64 `json:"tcpLargeExtraLossWeight"`
	LargeP95DegradationWeight    float64 `json:"tcpLargeP95DegradationWeight"`
	LargeCoverageWeight          float64 `json:"tcpLargeCoverageWeight"`
	ProfileMeanWeight            float64 `json:"tcpProfileMeanWeight"`
	ProfileP20Weight             float64 `json:"tcpProfileP20Weight"`
	MinimumRuns                  int     `json:"tcpMinimumRuns"`
	MinimumStandardSamples       int     `json:"tcpMinimumStandardSamples"`
	MinimumLargeSamples          int     `json:"tcpMinimumLargeSamples"`
	MinimumTargetCoveragePercent float64 `json:"tcpMinimumTargetCoverage"`
	ReferenceFailurePercent      float64 `json:"tcpReferenceFailureThreshold"`
	GuardWarningLossPercent      float64 `json:"tcpGuardWarningLoss"`
	GuardWarningMaximumScore     float64 `json:"tcpGuardWarningMaximumScore"`
	GuardCriticalLossPercent     float64 `json:"tcpGuardCriticalLoss"`
	GuardCriticalMaximumScore    float64 `json:"tcpGuardCriticalMaximumScore"`
	GuardSevereLossPercent       float64 `json:"tcpGuardSevereLoss"`
	GuardSevereMaximumScore      float64 `json:"tcpGuardSevereMaximumScore"`
	ExcellentThreshold           float64 `json:"tcpExcellentThreshold"`
	GoodThreshold                float64 `json:"tcpGoodThreshold"`
	FairThreshold                float64 `json:"tcpFairThreshold"`
}

func defaultTCPQualityScoreConfig() tcpQualityScoreConfig {
	return tcpQualityScoreConfig{
		ModelVersion:                 tcpQualityScoreModelVersion,
		OverallICMPWeight:            35,
		OverallStandardWeight:        55,
		OverallLargeWeight:           10,
		StandardLossWeight:           55,
		StandardP50Weight:            15,
		StandardP95Weight:            25,
		StandardCoverageWeight:       5,
		LargeLossWeight:              55,
		LargeExtraLossWeight:         25,
		LargeP95DegradationWeight:    15,
		LargeCoverageWeight:          5,
		ProfileMeanWeight:            70,
		ProfileP20Weight:             30,
		MinimumRuns:                  3,
		MinimumStandardSamples:       90,
		MinimumLargeSamples:          30,
		MinimumTargetCoveragePercent: 80,
		ReferenceFailurePercent:      70,
		GuardWarningLossPercent:      3,
		GuardWarningMaximumScore:     84.9,
		GuardCriticalLossPercent:     5,
		GuardCriticalMaximumScore:    69.9,
		GuardSevereLossPercent:       10,
		GuardSevereMaximumScore:      49.9,
		ExcellentThreshold:           95,
		GoodThreshold:                85,
		FairThreshold:                70,
	}
}

func loadTCPQualityScoreConfig() tcpQualityScoreConfig {
	defaults := defaultTCPQualityScoreConfig()
	theme, err := config.GetAs[string](config.ThemeKey, "default")
	if err != nil || theme == "" || theme == "default" {
		return defaults
	}
	var stored models.ThemeConfiguration
	if err := dbcore.GetDBInstance().Select("data").First(&stored, "short = ?", theme).Error; err != nil {
		return defaults
	}
	var settings map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stored.Data), &settings); err != nil {
		return defaults
	}
	version := 0
	_ = json.Unmarshal(settings["tcpQualityScoreModelVersion"], &version)
	if version != tcpQualityScoreModelVersion {
		return defaults
	}
	raw, err := json.Marshal(settings)
	if err != nil {
		return defaults
	}
	loaded := defaults
	if err := json.Unmarshal(raw, &loaded); err != nil {
		return defaults
	}
	return normalizeTCPQualityScoreConfig(loaded)
}

func normalizeTCPQualityScoreConfig(value tcpQualityScoreConfig) tcpQualityScoreConfig {
	defaults := defaultTCPQualityScoreConfig()
	value.ModelVersion = tcpQualityScoreModelVersion
	value.OverallICMPWeight = finiteClamp(value.OverallICMPWeight, 0, 100, defaults.OverallICMPWeight)
	value.OverallStandardWeight = finiteClamp(value.OverallStandardWeight, 0, 100, defaults.OverallStandardWeight)
	value.OverallLargeWeight = finiteClamp(value.OverallLargeWeight, 0, 100, defaults.OverallLargeWeight)
	if value.OverallICMPWeight+value.OverallStandardWeight+value.OverallLargeWeight <= 0 {
		value.OverallICMPWeight = defaults.OverallICMPWeight
		value.OverallStandardWeight = defaults.OverallStandardWeight
		value.OverallLargeWeight = defaults.OverallLargeWeight
	}
	value.StandardLossWeight = finiteClamp(value.StandardLossWeight, 0, 100, defaults.StandardLossWeight)
	value.StandardP50Weight = finiteClamp(value.StandardP50Weight, 0, 100, defaults.StandardP50Weight)
	value.StandardP95Weight = finiteClamp(value.StandardP95Weight, 0, 100, defaults.StandardP95Weight)
	value.StandardCoverageWeight = finiteClamp(value.StandardCoverageWeight, 0, 100, defaults.StandardCoverageWeight)
	if value.StandardLossWeight+value.StandardP50Weight+value.StandardP95Weight+value.StandardCoverageWeight <= 0 {
		value.StandardLossWeight = defaults.StandardLossWeight
		value.StandardP50Weight = defaults.StandardP50Weight
		value.StandardP95Weight = defaults.StandardP95Weight
		value.StandardCoverageWeight = defaults.StandardCoverageWeight
	}
	value.LargeLossWeight = finiteClamp(value.LargeLossWeight, 0, 100, defaults.LargeLossWeight)
	value.LargeExtraLossWeight = finiteClamp(value.LargeExtraLossWeight, 0, 100, defaults.LargeExtraLossWeight)
	value.LargeP95DegradationWeight = finiteClamp(value.LargeP95DegradationWeight, 0, 100, defaults.LargeP95DegradationWeight)
	value.LargeCoverageWeight = finiteClamp(value.LargeCoverageWeight, 0, 100, defaults.LargeCoverageWeight)
	if value.LargeLossWeight+value.LargeExtraLossWeight+value.LargeP95DegradationWeight+value.LargeCoverageWeight <= 0 {
		value.LargeLossWeight = defaults.LargeLossWeight
		value.LargeExtraLossWeight = defaults.LargeExtraLossWeight
		value.LargeP95DegradationWeight = defaults.LargeP95DegradationWeight
		value.LargeCoverageWeight = defaults.LargeCoverageWeight
	}
	value.ProfileMeanWeight = finiteClamp(value.ProfileMeanWeight, 0, 100, defaults.ProfileMeanWeight)
	value.ProfileP20Weight = finiteClamp(value.ProfileP20Weight, 0, 100, defaults.ProfileP20Weight)
	if value.ProfileMeanWeight+value.ProfileP20Weight <= 0 {
		value.ProfileMeanWeight = defaults.ProfileMeanWeight
		value.ProfileP20Weight = defaults.ProfileP20Weight
	}
	value.MinimumRuns = int(finiteClamp(float64(value.MinimumRuns), 1, 20, float64(defaults.MinimumRuns)))
	value.MinimumStandardSamples = int(finiteClamp(float64(value.MinimumStandardSamples), 10, 10000, float64(defaults.MinimumStandardSamples)))
	value.MinimumLargeSamples = int(finiteClamp(float64(value.MinimumLargeSamples), 10, 10000, float64(defaults.MinimumLargeSamples)))
	value.MinimumTargetCoveragePercent = finiteClamp(value.MinimumTargetCoveragePercent, 1, 100, defaults.MinimumTargetCoveragePercent)
	value.ReferenceFailurePercent = finiteClamp(value.ReferenceFailurePercent, 50, 100, defaults.ReferenceFailurePercent)
	value.GuardWarningLossPercent = finiteClamp(value.GuardWarningLossPercent, 0, 100, defaults.GuardWarningLossPercent)
	value.GuardCriticalLossPercent = finiteClamp(value.GuardCriticalLossPercent, value.GuardWarningLossPercent, 100, defaults.GuardCriticalLossPercent)
	value.GuardSevereLossPercent = finiteClamp(value.GuardSevereLossPercent, value.GuardCriticalLossPercent, 100, defaults.GuardSevereLossPercent)
	value.GuardWarningMaximumScore = finiteClamp(value.GuardWarningMaximumScore, 0, 100, defaults.GuardWarningMaximumScore)
	value.GuardCriticalMaximumScore = finiteClamp(value.GuardCriticalMaximumScore, 0, value.GuardWarningMaximumScore, defaults.GuardCriticalMaximumScore)
	value.GuardSevereMaximumScore = finiteClamp(value.GuardSevereMaximumScore, 0, value.GuardCriticalMaximumScore, defaults.GuardSevereMaximumScore)
	value.FairThreshold = finiteClamp(value.FairThreshold, 0, 100, defaults.FairThreshold)
	value.GoodThreshold = finiteClamp(value.GoodThreshold, value.FairThreshold, 100, defaults.GoodThreshold)
	value.ExcellentThreshold = finiteClamp(value.ExcellentThreshold, value.GoodThreshold, 100, defaults.ExcellentThreshold)
	return value
}

func finiteClamp(value, minimum, maximum, fallback float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		value = fallback
	}
	return math.Max(minimum, math.Min(maximum, value))
}

func weightedScore(values ...[2]float64) float64 {
	total, weights := 0.0, 0.0
	for _, value := range values {
		total += value[0] * value[1]
		weights += value[1]
	}
	if weights <= 0 {
		return 0
	}
	return total / weights
}
