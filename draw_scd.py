import pandas as pd
import matplotlib.pyplot as plt

df_scd_no_delta = pd.read_csv('3ld_lag.csv')
df_scd_s3delta = pd.read_csv('no_3ld_lag.csv')
df_scd = pd.read_csv('scd.csv')

time_scd_no_delta = df_scd_no_delta['Time']
lag_scd_no_delta = df_scd_no_delta['Lag'] * 1000

time_scd_s3delta = df_scd_s3delta['Time']
lag_scd_s3delta = df_scd_s3delta['Lag'] * 1000

time_scd = df_scd['Time']
lag_scd = df_scd['Lag'] * 1000

lag_scd_no_delta_mean = lag_scd_no_delta.expanding().mean()
lag_scd_s3delta_mean = lag_scd_s3delta.expanding().mean()
lag_scd_mean = lag_scd.expanding().mean()

# x start at 5
plt.xlim(10, 130)
# plt.ylim(0, 9000)
plt.ylim(0, 800)
plt.xticks(range(10, 140, 10))
# plt.yticks(range(0, 9500, 1000))
plt.yticks(range(0, 800, 50))

#plt.plot(time_scd_no_delta, lag_scd_no_delta, label='No SCD', color='blue')
plt.plot(time_scd_s3delta, lag_scd_s3delta, label='SCD + S3Delta', color='green')
plt.plot(time_scd, lag_scd, label='SCD + MemDelta', color='red')

#plt.plot(time_scd_no_delta, lag_scd_no_delta_mean, label='No SCD Mean', linestyle='--', color='blue')
plt.plot(time_scd_s3delta, lag_scd_s3delta_mean, label='SCD + S3Delta Mean', linestyle='--', color='green')
plt.plot(time_scd, lag_scd_mean, label='SCD + MemDelta Mean', linestyle='--', color='red')

#plt.annotate(f'{lag_scd_no_delta_mean[len(lag_scd_no_delta_mean)-1]:.2f}ms', xy=(123, 5000), xytext=(123, 5000), color='blue')
plt.annotate(f'{lag_scd_s3delta_mean[len(lag_scd_s3delta_mean)-1]:.2f}ms', xy=(123, 400), xytext=(123, 400), color='green')
plt.annotate(f'{lag_scd_mean[len(lag_scd_mean)-1]:.2f}ms', xy=(123, 100), xytext=(123, 180), color='red')


plt.title('Lag and Time\n(Throughput: about 500KB/s)', fontsize = 15)
plt.xlabel('Time (s)', fontsize = 20)
plt.ylabel('Lag (ms)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
