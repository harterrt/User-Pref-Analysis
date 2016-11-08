import ujson as json
import pandas as pd
import numpy as np
import plotly.plotly as py
from plotly.graph_objs import *
from collections import Counter

from moztelemetry import get_pings, get_pings_properties, get_one_ping_per_client, get_clients_history, get_records

def gp(sc):
    return get_pings(sc, app="Firefox", channel="nightly",
                     fraction=0.001)

def get_prefs(xx):
    xx['environment']['settings']['userPrefs']

def main(sc):
    pings = gp(sc)
    prefs = pings.map(get_prefs)
    counts = prefs.flatMap(lambda pref_map: pref_map.items()) \
         .map(lambda item: ((item[0], type(item[1])), 1)) \
         .reduceByKey(lambda a, b: a + b)

    with open('type_counts.pickle', 'w') as out:
        pickle.dump(counts, out)
