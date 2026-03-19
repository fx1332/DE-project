import requests
import pandas as pd
import time
import sys
from datetime import datetime

# ============================================================
# 1. 辅助函数：将 WMO 天气代码转换为中文描述
# ============================================================
def weather_code_to_text(code):
    """将 WMO 天气代码转换为中文描述"""
    codes = {
        0: "晴朗",
        1: "大部晴朗",
        2: "部分多云",
        3: "阴天",
        45: "雾",
        48: "雾凇",
        51: "轻度毛毛雨",
        53: "中度毛毛雨",
        55: "重度毛毛雨",
        56: "轻度冻毛毛雨",
        57: "重度冻毛毛雨",
        61: "小雨",
        63: "中雨",
        65: "大雨",
        66: "冻雨",
        67: "重度冻雨",
        71: "小雪",
        73: "中雪",
        75: "大雪",
        77: "雪粒",
        80: "小阵雨",
        81: "中阵雨",
        82: "强阵雨",
        85: "小阵雪",
        86: "大阵雪",
        95: "雷暴",
        96: "雷暴伴冰雹",
        99: "强雷暴伴冰雹"
    }
    return codes.get(code, f"未知代码({code})")

# ============================================================
# 2. 带重试机制的单个城市天气获取函数
# ============================================================
def fetch_weather_with_retry(city, max_retries=3, retry_delay=2):
    """
    对单个城市发起请求，失败时按固定间隔自动重试（最多 max_retries 次）。
    返回解析后的完整数据字典，若最终失败则返回 None。
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current_weather": True,
        "timezone": "Asia/Shanghai"
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()                # 如果状态码不是200，抛出HTTPError
            data = response.json()                     # 尝试解析JSON
            return data                                 # 成功则返回数据

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # 网络层问题：超时、连接错误 → 可以重试
            print(f"  ⏱️ {city['name']} 请求失败（尝试 {attempt+1}/{max_retries}）: {e}")
            if attempt < max_retries - 1:
                print(f"     等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                print(f"     已达最大重试次数，放弃。")

        except requests.exceptions.HTTPError as e:
            # HTTP错误：根据状态码决定是否重试
            status = response.status_code
            if status in [429, 500, 502, 503, 504]:
                # 限流或服务器临时错误 → 可以重试
                print(f"  ⚠️ {city['name']} 返回 {status}（尝试 {attempt+1}/{max_retries}）")
                if attempt < max_retries - 1:
                    # 如果服务器提供了 Retry-After 头部，优先使用它
                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        wait = int(retry_after)
                        print(f"     根据服务器指示，等待 {wait} 秒...")
                        time.sleep(wait)
                    else:
                        print(f"     等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
                else:
                    print(f"     已达最大重试次数，放弃。")
            else:
                # 其他4xx错误（如404、400）一般不重试
                print(f"  ❌ {city['name']} 不可恢复的错误 {status}，放弃。")
                return None

        except Exception as e:
            # 其他未知异常（如JSON解析错误、字段缺失等）不重试
            print(f"  ❌ {city['name']} 发生未知错误: {e}")
            return None

    # 所有重试用尽后仍未成功
    print(f"  ❌ {city['name']} 最终失败（已达最大重试次数）")
    return None

# ============================================================
# 3. 城市列表（可根据需要增删）
# ============================================================
cities = [
    {"name": "北京", "lat": 39.9042, "lon": 116.4074},
    {"name": "上海", "lat": 31.2304, "lon": 121.4737},
    {"name": "广州", "lat": 23.1291, "lon": 113.2644},
    {"name": "深圳", "lat": 22.5431, "lon": 114.0579},
    {"name": "成都", "lat": 30.5728, "lon": 104.0668},
    {"name": "武汉", "lat": 30.5928, "lon": 114.3055},
    {"name": "西安", "lat": 34.3416, "lon": 108.9398},
    {"name": "杭州", "lat": 30.2741, "lon": 120.1551},
    {"name": "重庆", "lat": 29.5630, "lon": 106.5516},
    {"name": "南京", "lat": 32.0603, "lon": 118.7969},
    {"name": "天津", "lat": 39.0851, "lon": 117.1994},
    {"name": "苏州", "lat": 31.2989, "lon": 120.5853},
    {"name": "厦门", "lat": 24.4798, "lon": 118.0895},
    {"name": "青岛", "lat": 36.0671, "lon": 120.3826},
    {"name": "大连", "lat": 38.9140, "lon": 121.6147},
]

# ============================================================
# 4. 主程序：循环抓取每个城市，收集数据
# ============================================================
base_url = "https://api.open-meteo.com/v1/forecast"
all_weather = []

for city in cities:
    print(f"正在抓取 {city['name']} 的天气...")
    data = fetch_weather_with_retry(city)

    if data is None:
        print(f"  ❌ {city['name']} 抓取失败（已跳过）")
        continue

    # 解析当前天气数据
    try:
        current = data["current_weather"]
        dt = datetime.fromisoformat(current["time"])
        weather_code = current["weathercode"]
        weather_record = {
            "城市": city["name"],
            "观测时间": dt.strftime("%Y-%m-%d %H:%M"),
            "温度(°C)": current["temperature"],
            "风速(km/h)": current["windspeed"],
            "风向(°)": current["winddirection"],
            "天气代码": weather_code,
            "天气描述": weather_code_to_text(weather_code)
        }
        all_weather.append(weather_record)
        print(f"  ✅ {city['name']} 成功")
    except KeyError as e:
        print(f"  ❌ {city['name']} 数据缺少必要字段: {e}（跳过）")
    except Exception as e:
        print(f"  ❌ {city['name']} 数据处理错误: {e}（跳过）")

    # 礼貌性暂停：无论成功失败，每次循环后等待1秒
    time.sleep(1)

# ============================================================
# 5. 保存结果为 CSV
# ============================================================
if all_weather:
    df = pd.DataFrame(all_weather)
    csv_filename = "data/multi_cities_weather.csv"
    df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
    print(f"\n✅ 批量抓取完成！数据已保存至 {csv_filename}")
    print("前几行预览：")
    print(df.head())
else:
    print("没有抓取到任何数据。")

print("程序执行完毕。")