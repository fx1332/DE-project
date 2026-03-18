import requests
import pandas as pd
from datetime import datetime

# 1. 获取数据
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 39.9042,
    "longitude": 116.4074,
    "current_weather": True,
    "timezone": "Asia/Shanghai"
}
response = requests.get(url, params=params)
data = response.json()

# 2. 提取当前天气字段
current = data["current_weather"]

# 处理时间格式（从 ISO 字符串转为更友好的格式）
time_str = current["time"]  # 例如 "2025-03-17T14:00"
dt = datetime.fromisoformat(time_str)
formatted_time = dt.strftime("%Y-%m-%d %H:%M")

# 准备要保存的数据字典
weather_record = {
    "纬度": data["latitude"],
    "经度": data["longitude"],
    "观测时间": formatted_time,
    "温度(°C)": current["temperature"],
    "风速(km/h)": current["windspeed"],
    "风向(°)": current["winddirection"],
    "天气代码": current["weathercode"]
}

# 3. 用 pandas 创建 DataFrame（一行数据）
df = pd.DataFrame([weather_record])

# 4. 保存为 CSV（不包含索引）
csv_filename = "data/weather_current.csv"
df.to_csv(csv_filename, index=False, encoding="utf-8-sig")

print(f"✅ 数据已保存到 {csv_filename}")
print(df)  # 打印出来看看