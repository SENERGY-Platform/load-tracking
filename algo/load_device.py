
import pandas as pd
import numpy as np

class Load:
    def __init__(self, power_series, energy_series):
        self.power_series = power_series
        self.consumed_energy = energy_series.iloc[-1] - energy_series.iloc[0]
        self.max_power = None
        self.length = None
        self.threshold = None
    
    def compute_features(self):
        self.max_power = self.power_series.max()
        self.length = self.power_series.index[-1] - self.power_series.index[0]
        self.threshold = self.power_series.quantile(q=0.5)


def extract_loads(power_series, energy_series):
    list_of_loads = []
    list_of_load_inds = []
    new_load = []
    end_check = []
    active = False
    for i in range(len(power_series)):
        if active == True:
            new_load.append(i)
            if power_series[i] < 1.5: # If power values are below 1.5 for more than 10 time steps the load has stopped
                end_check.append(i)
            if len(end_check) > 10:
                active = False
                list_of_load_inds.append(new_load[:-10])
                new_load = []
                end_check = []
        elif active == False:    
            if power_series[i] > 10:
                active = True
                if i < 10:
                    start_index = 0
                else:
                    start_index = i-10
                new_load.append(start_index)
    for load in list_of_load_inds:
        new_load = Load(power_series[load], energy_series[load])
        new_load.compute_features()
        list_of_loads.append(new_load)
    return list_of_loads


def online_tracking_loads(power_list, energy_list, list_of_loads, mean_features, active):
    if (active==False and power_list[-1][1] < 10) or (active==True and power_list[-1][1] > 1.5):
        return list_of_loads, mean_features, active
    if active==False and power_list[-1][1] > 10:
        active = True
        return list_of_loads, mean_features, active
    power_series = pd.Series(data=[data_point for _, data_point in power_list], index=[timestamp for timestamp, _ in power_list]).sort_index()
    power_series = power_series[~power_series.index.duplicated(keep='first')]
    energy_series = pd.Series(data=[data_point for _, data_point in energy_list], index=[timestamp for timestamp, _ in energy_list]).sort_index()
    energy_series = energy_series[~energy_series.index.duplicated(keep='first')]
    
    old_number_of_loads = len(list_of_loads)
    list_of_loads += extract_loads(power_series, energy_series)
    if len(list_of_loads) > old_number_of_loads:
        print("New load just ended!")
        active = False
    
    
    if len(list_of_loads) >= 1:
        mean_features['mean_consumed_energy'] = np.mean([load.consumed_energy for load in list_of_loads])
        mean_features['mean_max_power'] = np.mean([load.max_power for load in list_of_loads])
        mean_features['mean_length'] = np.mean([load.length.total_seconds() for load in list_of_loads])
        mean_features['mean_threshold'] = np.mean([load.threshold for load in list_of_loads])
    
    return list_of_loads, mean_features, active
    

    