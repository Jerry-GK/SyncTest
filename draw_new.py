import pandas as pd
import matplotlib.pyplot as plt

df_no_read_cdc = pd.read_csv('no_imd.csv')
df_read_cdc = pd.read_csv('with_imd.csv')

time_no_read_cdc = df_no_read_cdc['Time']
lag_no_read_cdc = df_no_read_cdc['Lag'] * 1000

time_read_cdc = df_read_cdc['Time']
lag_read_cdc = df_read_cdc['Lag'] * 1000

lag_no_read_cdc_mean = lag_no_read_cdc.expanding().mean()
lag_read_cdc_mean = lag_read_cdc.expanding().mean()

# x start at 5
plt.xlim(10, 130)
plt.ylim(0, 510)
plt.xticks(range(10, 130, 10))
plt.yticks(range(0, 510, 20))

plt.plot(time_no_read_cdc, lag_no_read_cdc, label='No IMD', color='blue')
plt.plot(time_read_cdc, lag_read_cdc, label='IMD', color='red')

plt.plot(time_no_read_cdc, lag_no_read_cdc_mean, label='No IMD Mean', linestyle='--', color='blue')
plt.plot(time_read_cdc, lag_read_cdc_mean, label='IMD Mean', linestyle='--', color='red')

plt.annotate(f'{lag_no_read_cdc_mean[len(lag_no_read_cdc_mean)-1]:.2f}s', xy=(120, 4), xytext=(125, 200), color='blue')
plt.annotate(f'{lag_read_cdc_mean[len(lag_read_cdc_mean)-1]:.2f}s', xy=(120, 1), xytext=(125, 180), color='red')


plt.title('Lag and Time\n(Throughput: about 550KB/s)', fontsize = 15)
plt.xlabel('Time (s)', fontsize = 20)
plt.ylabel('Lag (ms)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
