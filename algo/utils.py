import pandas as pd


def todatetime(timestamp):
        if str(timestamp).isdigit():
            if len(str(timestamp))==13:
                return pd.to_datetime(int(timestamp), unit='ms')
            elif len(str(timestamp))==19:
                return pd.to_datetime(int(timestamp), unit='ns')
        else:
            return pd.to_datetime(timestamp)
        
def compute_activation_score(mean_features, solar_forecast):
    mean_length = mean_features['mean_length']
    current_time = todatetime(solar_forecast[0][0])
    last_relevant_time_of_forecast_index = len(solar_forecast) - len([forecast_ts for forecast_ts, _ in solar_forecast if todatetime(forecast_ts)>=current_time+pd.Timedelta(mean_length,'seconds')])
    relevant_forecasts = [solar_forecast[i] for i in range(last_relevant_time_of_forecast_index)]
    threshold = mean_features['mean_threshold']
    if len(relevant_forecasts) >= 2:
        activation_score = (relevant_forecasts[0][1] - threshold)*(todatetime(relevant_forecasts[1][0])-pd.Timestamp.now(tz="UTC")).total_seconds()
        for i in range(1,len(relevant_forecasts)-1):
             activation_score += (relevant_forecasts[i][1] - threshold)*(todatetime(relevant_forecasts[i+1][0])-todatetime(relevant_forecasts[i][0])).total_seconds()
        activation_score += (relevant_forecasts[-1][1] - threshold)*(pd.Timestamp.now(tz="UTC") + mean_length - todatetime(relevant_forecasts[-1][0])).total_seconds()
    elif len(relevant_forecasts) == 1:
         activation_score = (relevant_forecasts[0][1] - threshold)*mean_length
    else:
         activation_score = 0 
    return activation_score
