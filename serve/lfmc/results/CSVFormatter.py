import pandas as pd
import xarray as xr


class CSVFormatter:

    @staticmethod
    async def format(data, variable):

        csvfile = '/FuelModels/queries/{}_{}.csv'.format(variable, ts)
        try:
            data[variable].to_csv(csvfile)
        except ValueError as e:
            logger.error(e)
        return csvfile
