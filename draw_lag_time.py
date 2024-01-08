import pandas as pd
import matplotlib.pyplot as plt

df_new = pd.read_csv('3ld_lag_3.csv')
Xmax = 601

time = df_new['Time'][0:Xmax]
lag_new = df_new['Lag'][0:Xmax] * 1000
lag_base = [0] * len(time)

lag_mean_new = lag_new.expanding().mean()

# x start at 0
plt.xlim(0, Xmax)
plt.ylim(-300, 1000)
plt.xticks(range(0, Xmax, 60))
plt.yticks(range(-300, 1000, 200))


plt.plot(time, lag_new, label='Lag', color='blue')
plt.plot(time, lag_mean_new, label='Lag Mean', linestyle='--', color='blue')
plt.plot(time, lag_base, linestyle='--', color='black')

plt.annotate(f'{lag_mean_new[len(lag_mean_new)-1]:.1f}ms', xy=(Xmax, 300), xytext=(Xmax, 300), color='blue')

plt.title('Lag and Time\n(Base table size: 100w, FlushInterval = 60s,\n QPS = 3100, 6750Kb/s)', fontsize = 15)
plt.xlabel('Time(s)', fontsize = 20)
plt.ylabel('Scan lag (ms)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
