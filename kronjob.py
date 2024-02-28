from helper import *
import extractv2
import preprocessv2
import Merge
import postprocess

extractv2.extract()
preprocessv2.main_preprocess()
Merge.merge_csv_files()
postprocess.postprocessv2()
extractv2.reset()