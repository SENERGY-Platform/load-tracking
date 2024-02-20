import pandas as pd


def todatetime(timestamp):
        if str(timestamp).isdigit():
            if len(str(timestamp))==13:
                return pd.to_datetime(int(timestamp), unit='ms')
            elif len(str(timestamp))==19:
                return pd.to_datetime(int(timestamp), unit='ns')
        else:
            return pd.to_datetime(timestamp)
        
def check_if_solar_power_sufficient(mean_features, solar_forecast):
    mean_length = mean_features['mean_length']
    current_time = todatetime(solar_forecast[0][0])
    last_relevant_time_of_forecast_index = len(solar_forecast) - len([forecast_ts for forecast_ts, _ in solar_forecast if todatetime(forecast_ts)>=current_time+pd.Timedelta(mean_length,'seconds')])
    relevant_forecasts = [solar_forecast[i] for i in range(last_relevant_time_of_forecast_index)]
    threshold = mean_features['mean_threshold']
    if [forecast for _, forecast in relevant_forecasts if forecast < threshold] == []:
         activate_device = True
    else:
         activate_device = False 
    return activate_device
