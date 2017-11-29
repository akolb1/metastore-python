# noinspection PyCompatibility

try:
    import statistics
except ImportError:
    import numpy as statistics


class Statistics(object):
    """
    Provide common methods for manipulating statistics
    """

    MARGIN = 2

    def __init__(self, data=None):
        self.__data = data if data else []

    @property
    def data(self):
        return self.__data

    def add(self, delta):
        self.__data.append(delta)
        return self

    @property
    def mean(self):
        return statistics.mean(self.data)

    @property
    def harmonic_mean(self):
        return statistics.harmonic_mean(self.data)

    @property
    def median(self):
        return statistics.median(self.data)

    @property
    def min(self):
        return min(self.data)

    @property
    def max(self):
        return max(self.data)

    @property
    def stdev(self):
        return statistics.stdev(self.data)

    @property
    def variance(self):
        return statistics.variance(self.data)

    @property
    def pvariance(self):
        return statistics.pvariance(self.data)

    def sanitize(self):
        """
        Return sanitized object with data outliers removed.
        An outlier is outside of +/- 2 * stddev from mean.

        :return: Sanitized statistic
        """
        mean_value = self.mean
        delta = self.MARGIN * self.stdev
        min_val = mean_value - delta
        max_val = mean_value + delta
        new_data = [x for x in self.data if (min_val < x < max_val)]
        return Statistics(new_data)

    def write(self, name):
        """
        Write data to the specified file

        :param name: file name
        :type name str
        """
        with open(name, "w") as f:
            for v in self.data:
                f.write(str(v) + "\n")
