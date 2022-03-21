# Thunderbolt

[![Test](https://github.com/m3dev/thunderbolt/workflows/Test/badge.svg)](https://github.com/m3dev/thunderbolt/actions?query=workflow%3ATest)
[![Python Versions](https://img.shields.io/pypi/pyversions/thunderbolt.svg)](https://pypi.org/project/thunderbolt/)
[![](https://img.shields.io/pypi/v/thunderbolt)](https://pypi.org/project/thunderbolt/)
![](https://img.shields.io/pypi/l/thunderbolt)

Thunderbolt is data manager for gokart.


1. Auto loading gokart task logs
1. Check task params using pandas
1. Download data from python


# Usage

### install
```shell
pip install thunderbolt
```

### Example

If you specify `TASK_WORKSPACE_DIRECTORY`, thunderbolt reads the log.
So making tasks pandas.DataFrame, and load dumped data.
This is also possible from S3 or GCS. (s3://, gs://)

Example:
```python
from thunderbolt import Thunderbolt

tb = Thunderbolt()
print(tb.get_task_df())
print(tb.get_data('TASK_NAME'))
```

Please look here too: https://github.com/m3dev/thunderbolt/blob/master/examples/example.ipynb


# Thanks

- `gokart`: https://github.com/m3dev/gokart
