package balancer

import "math"

func sum(stats []int) (sum float64, count float64) {
	sum = 0
	count = float64(len(stats))

	if count > 0 {
		for i := 0; i < len(stats); i++ {
			sum += float64(stats[i])
		}
	}
	return
}

func avg(stats []int) float64 {
	sum, count := sum(stats)
	if count == 0 {
		return 0
	}
	return sum / count
}

func mean(stats []int) float64 {
	if len(stats) == 0 {
		return 0
	}
	return float64(stats[len(stats)/2])
}

func variance(stats []int) float64 {
	count := len(stats)
	if count < 2 {
		return 0
	}

	mean := mean(stats)
	var s1 float64 = 0
	for i := 0; i < count; i++ {
		s1 += math.Pow(float64(stats[i])-mean, 2)
	}

	return s1 / (float64(count) - 1)
}

func stddev(stats []int) float64 {
	return math.Sqrt(variance(stats))
}
