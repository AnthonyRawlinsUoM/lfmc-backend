class QueryAnalyzer:
    
    """ Checks spatial and temporal bounds of query against indexed data """
    
    # creates an ingestion task for missing extents
    
    
    # Only extends in 1-Dimension (Time) bi-directionally ie., future (present) & past (historical)
    
    # Spatial Bounds are limited to Australia GDA94 (null values for spatial components outside these bounds)