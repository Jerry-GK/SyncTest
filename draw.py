import pandas as pd
import matplotlib.pyplot as plt

df_old_5s = pd.read_csv('old_5s.csv')
df_old_10s = pd.read_csv('old_10s.csv')
df_new = pd.read_csv('new.csv')
df_new_no_stats = pd.read_csv('new_no_stats.csv')
df_scd = pd.read_csv('no_read_cdc_real.csv')

time_old_5s = df_old_5s['Time']
lag_old_5s = df_old_5s['Lag']

time_old_10s = df_old_10s['Time']
lag_old_10s = df_old_10s['Lag']

time_new = df_new['Time']
lag_new = df_new['Lag']

time_new_no_stats = df_new_no_stats['Time']
lag_new_no_stats = df_new_no_stats['Lag']

time_scd = df_scd['Time']
lag_scd = df_scd['Lag']

lag_old_5s_mean = lag_old_5s.expanding().mean()
lag_old_10s_mean = lag_old_10s.expanding().mean()
lag_new_mean = lag_new.expanding().mean()
lag_new_no_stats_mean = lag_new_no_stats.expanding().mean()
lag_scd_mean = lag_scd.expanding().mean()


# x start at 10
plt.xlim(10, 215)
plt.ylim(0, 80)
plt.xticks(range(10, 215, 10))
plt.yticks(range(0, 82, 2))

plt.plot(time_old_5s, lag_old_5s, label='Old (flushInterval = 5s)', color='red')
plt.plot(time_old_10s, lag_old_10s, label='Old (flushInterval = 10s)', color='darkorange')
plt.plot(time_new, lag_new, label='ORC + CaC', color='green')
plt.plot(time_new_no_stats, lag_new_no_stats, label='ORC + CaC (no statistic)', color='blue')
plt.plot(time_scd, lag_scd, label = 'SCD', color='purple')

plt.plot(time_old_5s, lag_old_5s_mean, label='Old (flushInterval = 5s) Mean', linestyle='--', color='red')
plt.plot(time_old_10s, lag_old_10s_mean, label='Old (flushInterval = 10s) Mean', linestyle='--', color='darkorange')
plt.plot(time_new, lag_new_mean, label='ORC + CaC Mean', linestyle='--', color='green')
plt.plot(time_new_no_stats, lag_new_no_stats_mean, label='ORC + CaC (no statistic) Mean', linestyle='--', color='blue')
plt.plot(time_scd, lag_scd_mean, label = 'SCD Mean', linestyle='--', color='purple')

plt.annotate(f'{lag_old_5s_mean[len(lag_old_5s_mean)-1]:.2f}s', xy=(200, 10), xytext=(205, 35), color='red')
plt.annotate(f'{lag_old_10s_mean[len(lag_old_10s_mean)-1]:.2f}s', xy=(200, 20), xytext=(205, 22), color='darkorange')
plt.annotate(f'{lag_new_mean[len(lag_new_mean)-1]:.2f}s', xy=(200, 30), xytext=(205, 11), color='green')
plt.annotate(f'{lag_new_no_stats_mean[len(lag_new_no_stats_mean)-1]:.2f}s', xy=(200, 40), xytext=(209, 4), color='blue')
plt.annotate(f'{lag_scd_mean[len(lag_scd_mean)-1]:.2f}s', xy=(200, 50), xytext=(209, 2), color='purple')


plt.title('Lag and Time\n(5500 updates/s on 10w rows table)', fontsize = 15)
plt.xlabel('Time (s)', fontsize = 20)
plt.ylabel('Lag (s)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
