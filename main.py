"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

__all__ = ("Operator", )

from operator_lib.util import OperatorBase, Selector
import os
import pickle
from algo import load_device, utils

from operator_lib.util import Config
class CustomConfig(Config):
    data_path = "/opt/data"


class Operator(OperatorBase):
    configType = CustomConfig

    selectors = [
        Selector({"name": "device_data", "args": ["time", "energy", "power"]}), 
        Selector({"name": "solar_forecast", "args": ["solar_forecast", "solar_forecast_timestamp"]})
    ]

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        data_path = self.config.data_path

        if not os.path.exists(data_path):
            os.mkdir(data_path)

        self.power_list = []
        self.energy_list = []

        self.active = False

        self.list_of_loads = []

        self.mean_features = {'mean_consumed_energy': 0, 'mean_max_power': 0, 'mean_length': 0, 'mean_threshold': 0} # mean_length is given in seconds

        self.loads_path = f'{data_path}/loads.pickle'
        self.mean_features_path = f'{data_path}/mean_features.pickle'

        self.results_from_same_weather_forecast = []

        self.load_data()

    def save_data(self):
        with open(self.loads_path, 'wb') as f:
            pickle.dump(self.list_of_loads, f)
        with open(self.mean_features_path, 'wb') as f:
            pickle.dump(self.mean_features, f)

    def load_data(self):
        if os.path.exists(self.loads_path):
            with open(self.loads_path, 'rb') as f:
                self.list_of_loads = pickle.load(f)
        if os.path.exists(self.mean_features_path):
            with open(self.mean_features_path, 'rb') as f:
                self.mean_features = pickle.load(f)
      
    def run(self, data, selector):
        print(f"{selector}  :   {str(data)}")
        #print(f"{selector}  :   {str(data)}")
        if selector == "device_data":
            timestamp = utils.todatetime(data["time"])
            energy = float(data["energy"])
            power = float(data["power"])
            self.energy_list.append([timestamp, energy])
            self.power_list.append([timestamp, power])
            #print('energy: '+str(energy)+'  '+'power: '+str(power)+'   '+'time: '+str(timestamp))
            old_number_of_loads = len(self.list_of_loads)
            self.power_list, self.energy_list, self.list_of_loads, self.mean_features, self.active = load_device.online_tracking_loads(self.power_list, self.energy_list, self.list_of_loads, self.mean_features, self.active)
            if len(self.list_of_loads) > old_number_of_loads:
                self.save_data()
                self.energy_list = []
                self.power_list = []
        elif selector == "solar_forecast":
            if len(self.results_from_same_weather_forecast)<47:
                self.results_from_same_weather_forecast.append(data)
            elif len(self.results_from_same_weather_forecast)==47:
                self.results_from_same_weather_forecast.append(data)
                solar_forecast = [(data["solar_forecast_timestamp"], data["solar_forecast"]) for data in self.results_from_same_weather_forecast]
                self.results_from_same_weather_forecast = []
                if len(self.list_of_loads) > 0:
                    activate_device = utils.check_if_solar_power_sufficient(self.mean_features, solar_forecast)
                    print(f"Activate Device: {activate_device}")
                    return {'activate_device': activate_device}

from operator_lib.operator_lib import OperatorLib
if __name__ == "__main__":
    OperatorLib(Operator(), name="load-tracking-operator", git_info_file='git_commit')