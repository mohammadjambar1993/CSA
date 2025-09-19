
`
# DataFilters Module

The `DataFilters` module provides a flexible way to apply various data filtering techniques, including median filtering, Gaussian filtering, and percentile filtering, to NumPy arrays or lists. This module is designed for data management tasks, making it easier to preprocess data before analysis.

## Features

- **Median Filtering**: Reduces noise by replacing each pixel with the median value of its neighbors.
- **Gaussian Filtering**: Smooths the data using a Gaussian kernel, which is effective for removing high-frequency noise.
- **Percentile Filtering**: Sets values below a certain percentile threshold to zero, allowing for the removal of outliers.

## Class: `DataFilters`

### Initialization

You can create an instance of the `DataFilters` class, where you can set various parameters. If no parameters are specified, default values will be used.

```python
filters = DataFilters(
    median_filter_bool=True,      # Whether to apply median filtering
    median_size=3,                # Size of the median filter
    gaussian_filter_bool=True,     # Whether to apply Gaussian filtering
    gaussian_sigma=1,             # Sigma for Gaussian filter
    percentile_filter_bool=False,  # Whether to apply percentile filtering
    percentile_threshold=5         # Percentile threshold for filtering
)
```

### Parameters

- `median_filter_bool` (bool): Enable or disable median filtering (default: `True`).
- `median_size` (int): Size of the median filter (default: `3`).
- `gaussian_filter_bool` (bool): Enable or disable Gaussian filtering (default: `True`).
- `gaussian_sigma` (float): Standard deviation for Gaussian filter (default: `1`).
- `percentile_filter_bool` (bool): Enable or disable percentile filtering (default: `False`).
- `percentile_threshold` (float): Percentile threshold for filtering (default: `5`).

### Getters and Setters

You can access and modify the filter parameters using getter and setter methods.

```python
filters.median_filter_bool = False  # Disable median filtering
print(filters.median_size)            # Get the current median size
```

### Method: `apply_data_filters`

To apply the filters to your data, use the `apply_data_filters` method. It accepts a NumPy array or a list.

```python
filtered_data = filters.apply_data_filters(data_matrix)
```

#### Parameters
- `data_matrix`: A NumPy array or a list of data to be filtered. If a list is provided, it will be reshaped to (15, 24).

#### Returns
- Returns a filtered NumPy array based on the applied filters.

### Example Usage

Here’s an example of how to use the `DataFilters` class:

```python
import numpy as np
from bleusb_comm_toolkit import DataFilters  # Assuming DataFilters is in data_management module

# Create an instance of DataFilters with custom parameters
filters = DataFilters(median_size=5, gaussian_sigma=2)

# Create a sample data matrix
data_matrix = np.random.rand(15, 24)

# Apply the data filters
filtered_data = filters.apply_data_filters(data_matrix)

# Print the result
print(filtered_data)
```

## Installation

To use this module, ensure you have the required libraries installed. You can install them using pip:

```bash
pip install numpy scipy
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request to enhance this module.

## Author

Susan Peters
susan.peters@myant.ca 
```


