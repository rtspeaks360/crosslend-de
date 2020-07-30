# -*- coding: utf-8 -*-
# @Author: rish
# @Date:   2020-07-29 16:34:33
# @Last Modified by:   rish
# @Last Modified time: 2020-07-29 16:35:23

### Imports START
import os
import sys
import inspect
### Imports END

# Changing system path to parent directory
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(
	inspect.currentframe()
)))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

# import config from parent directory
import config