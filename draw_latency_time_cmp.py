import pandas as pd
import matplotlib.pyplot as plt

df_1 = pd.read_csv('3ld_latency_join_old.csv')
df_2 = pd.read_csv('3ld_latency_join_new2.csv')
Xmax = 500
Ylimit = 1500

time_1 = df_1['Time'][0:Xmax]
time_2 = df_2['Time'][0:Xmax]
latency_1 = df_1['Scan'][0:Xmax] * 1000
latency_2 = df_2['Scan'][0:Xmax] * 1000
latency_base = [75] * len(time_1)

latency_mean_1 = latency_1.expanding().mean()
latency_mean_2 = latency_2.expanding().mean()

# x start at 0
plt.xlim(0, Xmax)
plt.ylim(0, Ylimit)
plt.xticks(range(0, Xmax, 60))
plt.yticks(range(0, Ylimit, 50))


plt.plot(time_1, latency_1, label='Latency', color='blue')
plt.plot(time_1, latency_mean_1, label='Latency Mean', linestyle='--', color='blue')

plt.plot(time_2, latency_2, label='Latency', color='red')
plt.plot(time_2, latency_mean_2, label='Latency Mean', linestyle='--', color='red')

plt.plot(time_1, latency_base, label='No SCD Latency', color='green')

plt.annotate(f'{latency_base[0]}ms', xy=(0, 71), xytext=(-10, 71), color='green')
plt.annotate(f'{latency_1[0]}ms', xy=(0, 210), xytext=(-10, 210), color='red')
plt.annotate(f'{latency_mean_1[len(latency_mean_1)-1]:.1f}ms', xy=(Xmax, 760), xytext=(Xmax, 760), color='red')

plt.title('Scan Latency and Delta Size\n(Base table size: 100w, FlushInterval = 60s,\n QPS = 3100, 6750Kb/s)', fontsize = 15)
plt.xlabel('Time(s)', fontsize = 20)
plt.ylabel('Scan Latency (ms)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
