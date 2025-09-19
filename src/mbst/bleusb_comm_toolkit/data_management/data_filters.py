import numpy as np
from scipy.ndimage import gaussian_filter, median_filter


class DataFilters:
    def __init__(
        self,
        median_filter_bool=True,
        median_size=3,
        gaussian_filter_bool=True,
        gaussian_sigma=1,
        percentile_filter_bool=False,
        percentile_threshold=5,
    ):
        self._median_filter_bool = median_filter_bool
        self._median_size = median_size
        self._gaussian_filter_bool = gaussian_filter_bool
        self._gaussian_sigma = gaussian_sigma
        self._percentile_filter_bool = percentile_filter_bool
        self._percentile_threshold = percentile_threshold

    # Getter and setter for median_filter_bool
    @property
    def median_filter_bool(self):
        return self._median_filter_bool

    @median_filter_bool.setter
    def median_filter_bool(self, value):
        self._median_filter_bool = value

    # Getter and setter for median_size
    @property
    def median_size(self):
        return self._median_size

    @median_size.setter
    def median_size(self, value):
        self._median_size = value

    # Getter and setter for gaussian_filter_bool
    @property
    def gaussian_filter_bool(self):
        return self._gaussian_filter_bool

    @gaussian_filter_bool.setter
    def gaussian_filter_bool(self, value):
        self._gaussian_filter_bool = value

    # Getter and setter for gaussian_sigma
    @property
    def gaussian_sigma(self):
        return self._gaussian_sigma

    @gaussian_sigma.setter
    def gaussian_sigma(self, value):
        self._gaussian_sigma = value

    # Getter and setter for percentile_filter_bool
    @property
    def percentile_filter_bool(self):
        return self._percentile_filter_bool

    @percentile_filter_bool.setter
    def percentile_filter_bool(self, value):
        self._percentile_filter_bool = value

    # Getter and setter for percentile_threshold
    @property
    def percentile_threshold(self):
        return self._percentile_threshold

    @percentile_threshold.setter
    def percentile_threshold(self, value):
        self._percentile_threshold = value

    def apply_data_filters(self, data_matrix):
        final_frame = None
        reshaped_array = None

        if isinstance(data_matrix, np.ndarray):
            reshaped_array = data_matrix
        elif isinstance(data_matrix, list):
            np_array = np.array(data_matrix)
            reshaped_array = np_array.reshape((15, 24))
        else:
            print("Data is neither a numpy array nor a list.")
            return None

        if (
            self._median_filter_bool
            or self._gaussian_filter_bool
            or self._percentile_filter_bool
        ):
            filter_size = self._median_size
            median_frame = median_filter(reshaped_array, size=filter_size)
            final_frame = median_frame

        if self._gaussian_filter_bool or self._percentile_filter_bool:
            sigma = self._gaussian_sigma
            gaussian_frame = gaussian_filter(median_frame, sigma=sigma)
            final_frame = gaussian_frame

        if self._percentile_filter_bool:
            lower_percentile = self._percentile_threshold
            lower_threshold = np.percentile(gaussian_frame, lower_percentile)
            percentile_frame = np.where(
                gaussian_frame < lower_threshold, 0, gaussian_frame
            )
            final_frame = percentile_frame

        if final_frame is None:
            final_frame = data_matrix

        return final_frame
