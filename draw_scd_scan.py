import pandas as pd
import matplotlib.pyplot as plt

df_scd_no_delta = pd.read_csv('scd_scan_no_delta.csv')
df_scd_s3delta = pd.read_csv('scd_scan_s3delta.csv')
df_scd = pd.read_csv('scd_scan.csv')

time_scd_no_delta = df_scd_no_delta['Time']
scan_scd_no_delta = df_scd_no_delta['Scan'] * 1000

time_scd_s3delta = df_scd_s3delta['Time']
scan_scd_s3delta = df_scd_s3delta['Scan'] * 1000

time_scd = df_scd['Time']
scan_scd = df_scd['Scan'] * 1000

scan_scd_no_delta_mean = scan_scd_no_delta.expanding().mean()
scan_scd_s3delta_mean = scan_scd_s3delta.expanding().mean()
scan_scd_mean = scan_scd.expanding().mean()

# x start at 5
plt.xlim(10, 130)
plt.ylim(0, 800)
plt.xticks(range(10, 140, 10))
plt.yticks(range(0, 800, 100))

plt.plot(time_scd_no_delta, scan_scd_no_delta, label='No SCD', color='blue')
# plt.plot(time_scd_s3delta, scan_scd_s3delta, label='SCD + S3Delta', color='green')
plt.plot(time_scd, scan_scd, label='SCD + MemDelta', color='red')

plt.plot(time_scd_no_delta, scan_scd_no_delta_mean, label='No SCD Mean', linestyle='--', color='blue')
# plt.plot(time_scd_s3delta, scan_scd_s3delta_mean, label='SCD + S3Delta Mean', linestyle='--', color='green')
plt.plot(time_scd, scan_scd_mean, label='SCD + MemDelta Mean', linestyle='--', color='red')

plt.annotate(f'{scan_scd_no_delta_mean[len(scan_scd_no_delta_mean)-1]:.2f}ms', xy=(120, 4), xytext=(123, 80), color='blue')
# plt.annotate(f'{scan_scd_s3delta_mean[len(scan_scd_s3delta_mean)-1]:.2f}ms', xy=(120, 2), xytext=(123, 1700), color='green')
plt.annotate(f'{scan_scd_mean[len(scan_scd_mean)-1]:.2f}ms', xy=(120, 1), xytext=(123, 200), color='red')


plt.title('Table Scan Latency and Time\n(Throughput: about 500KB/s)\n(Query: SELECT MAX(time) FROM test)', fontsize = 15)
plt.xlabel('Time (s)', fontsize = 20)
plt.ylabel('Table Scan Latency (ms)', fontsize = 20)

plt.legend()

plt.savefig('scan_time_plot.png')

plt.show()
