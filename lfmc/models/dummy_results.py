from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.results.DataPoint import DataPoint
import numpy as np
import pandas as pd
import datetime as dt

class DummyResults:

    @staticmethod
    def dummy_series():
        response = []
        for i in range(1, 30):

            # Dummy data for testing...
            # value, mean, min, max, std
            five_values = [np.random.random_sample() for j in range(5)]
            
            five_values = pd.DataFrame(five_values)
            # print(five_values)
            response.append(DataPoint(dt.date(2018, 1, i).strftime("%Y-%m-%dT00:00:00.000Z"),
                                      five_values[0].mean(),
                                      five_values[0].mean(),
                                      five_values[0].min(),
                                      five_values[0].max(),
                                      five_values[0].std()
                                      ))
            
        return response

    @staticmethod
    def dummy_single(v):
        # Dummy data for testing...
        # value, mean, min, max, std
        five_values = [np.random.random_sample() for j in range(5)]

        five_values = pd.DataFrame(five_values)
        # print(five_values)
        return DataPoint(dt.date(2018, 1, v+1).strftime("%Y-%m-%dT00:00:00.000Z"),
                                  five_values[0].mean(),
                                  five_values[0].mean(),
                                  five_values[0].min(),
                                  five_values[0].max(),
                                  five_values[0].std()
                                  )
