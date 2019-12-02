# Thunderbolt

Thunderbolt is data manager for gokart.

  
 [1] Auto loading gokart task logs  
 [2] Check task params using pandas  
 [3] Download data from python  
  

# Usage

### install
```
pip install thunderbolt
```

### Example

If you specify `TASK_WORKSPACE_DIRECTORY`, thunderbolt reads the log.  
So making tasks pandas.DataFrame, and load dumped data.  
This is also possible from S3 or GCS. (s3://~~, gs://~~)  
  
Example:
```
from thunderbolt import Thunderbolt

tb = Thunderbolt(PATH_TO_WORKSPACE_DIRECTORY)
print(tb.get_task_df())  # check task_id
print(tb.load(task_id=0))
```

Please look here too: https://github.com/m3dev/thunderbolt/blob/master/examples/example.ipynb  
  

# Thanks

- `gokart`: https://github.com/m3dev/gokart
