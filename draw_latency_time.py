import pandas as pd
import matplotlib.pyplot as plt

df_new = pd.read_csv('3ld_latency_tmp2.csv')
Xmax = 3000
Ylimit = 3000

time = df_new['Time'][0:Xmax]
latency_new = df_new['Scan'][0:Xmax] * 1000
latency_base = [45] * len(time)

latency_mean_new = latency_new.expanding().mean()

# x start at 0
plt.xlim(0, Xmax)
plt.ylim(0, Ylimit)
plt.xticks(range(0, Xmax, 30))
plt.yticks(range(0, Ylimit, 100))


plt.plot(time, latency_new, label='Latency', color='red')
plt.plot(time, latency_mean_new, label='Latency Mean', linestyle='--', color='red')

# plt.plot(time, latency_base, label='No SCD Latency', color='green')

# plt.annotate(f'{latency_base[0]}ms', xy=(0, 71), xytext=(-10, 71), color='green')
plt.annotate(f'{latency_new[0]}ms', xy=(0, 210), xytext=(-10, 210), color='red')
plt.annotate(f'{latency_mean_new[len(latency_mean_new)-1]:.1f}ms', xy=(Xmax, 760), xytext=(Xmax, 760), color='red')

# plt.title('Scan Latency and Delta Size\n(Base table size: 100w, FlushInterval = 60s,\n QPS = 3100, 6750Kb/s)', fontsize = 15)
plt.title('Scan Latency and Time(No Short Circuit)\n(Base table size: 100w, Merge Interval = 30s,\n QPS = 4200, 8200Kb/s)', fontsize = 15)
plt.xlabel('Time(s)', fontsize = 20)
plt.ylabel('Scan Latency (ms)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
