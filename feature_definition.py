# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    ValueType,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float64, Int64, String


driver_id = Entity(name="Driver_Id", 
                   join_keys=["id"],
                   value_type=ValueType.STRING, 
                   description="ID of the driver"
                   )

driver_stats_source = FileSource(
    name="driver_stats_source",
    path="data/predictor_df.parquet",
    timestamp_field="event_timestamp"
)

driver_stats_fv = FeatureView(
    name="driver_stats",
    entities=[driver_id],
    ttl=timedelta(days=2),
    schema=[
        Field(name="hour", dtype=Int64),
        Field(name="day", dtype=Int64),
        Field(name="month", dtype=Int64),
        Field(name="source", dtype=Int64),
        Field(name="destination", dtype=Int64),
        Field(name="cab_type", dtype=Int64),
        Field(name="product_id", dtype=Int64),
        Field(name="name", dtype=Int64),
        Field(name="distance", dtype=Float64),
        Field(name="surge_multiplier", dtype=Float64),
        Field(name="latitude", dtype=Float64),
        Field(name="longitude", dtype=Float64),
        Field(name="temperature", dtype=Float64),
        Field(name="short_summary", dtype=Int64),
        Field(name="long_summary", dtype=Int64),
        Field(name="apparentTemperatureLow", dtype=Float64),
        Field(name="windBearing", dtype=Int64),
        Field(name="cloudCover", dtype=Float64),
        Field(name="temperatureMin", dtype=Float64),
        Field(name="temperatureMax", dtype=Float64),
        Field(name="apparentTemperatureMin", dtype=Float64),
        Field(name="apparentTemperatureMax", dtype=Float64),
    ],
    source=driver_stats_source,
    online=True,
    tags={"driver": "driver_performance"}
)

# Target Feature View

target_source = FileSource(
    path="data/target_df.parquet",
    timestamp_field="event_timestamp"
)

target_stats_fv = FeatureView(
    name="target_feature_view",
    entities= [driver_id],
    ttl=timedelta(days=2),
    schema=[
        Field(name="price", dtype=Float64),
    ],
    source=target_source,
    online=True,
    tags={"journey_cost": "price"}
)

