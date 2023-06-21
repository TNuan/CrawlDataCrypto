import pandas as pd

# Đọc file CSV gốc
df = pd.read_csv('./data_BTC/LABEL_BTC.csv')

# Chuyển cột 'date' sang kiểu datetime
df['OPEN_TIME'] = pd.to_datetime(df['OPEN_TIME'], unit='ms')

# Tạo một DataFrame mới với cột 'date' chứa tất cả các giá trị thời gian từ thời điểm đầu đến thời điểm cuối trong khoảng thời gian cần bổ sung
start_date = df['OPEN_TIME'].min()
end_date = df['OPEN_TIME'].max()
date_range = pd.date_range(start=start_date, end=end_date, freq='1H')

new_df = pd.DataFrame({'OPEN_TIME': date_range})
new_df['date'] = pd.to_datetime(new_df['OPEN_TIME'], unit='ms')

# Merge DataFrame mới với DataFrame gốc dựa trên cột 'date'

merged_df = pd.merge(new_df, df, how='left')
merged_df = merged_df.drop('OPEN_TIME', axis=1)


# # Điền các giá trị bị thiếu bằng phương thức forward fill
merged_df = merged_df.ffill()

# Lưu DataFrame mới vào file CSV
merged_df.to_csv('LABEL_BTC_time_fill.csv', index=False)