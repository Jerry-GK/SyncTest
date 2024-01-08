import pandas as pd
import matplotlib.pyplot as plt

#df = pd.read_csv('3ld_100w_delta_latency.csv')
df_new = pd.read_csv('3ld_100w_delta_latency_new.csv')

#delta_size = df['Delta Size']
delta_size_new = df_new['Delta Size'] / 10000
#latency = df['Scan Latency'] * 1000
latency_new = df_new['Scan Latency'] * 1000

#latency_mean = latency.expanding().mean()
latency_mean_new = latency_new.expanding().mean()

# x start at 0
plt.xlim(0, 70)
plt.ylim(0, 6500)
plt.xticks(range(0, 70, 5))
plt.yticks(range(0, 6500, 500))

#plt.plot(delta_size, latency, label='Latency', color='blue')
#plt.plot(delta_size, latency_mean, label='Latency Mean', linestyle='--', color='blue')

plt.plot(delta_size_new, latency_new, label='Latency', color='red')
plt.plot(delta_size_new, latency_mean_new, label='Latency Mean', linestyle='--', color='red')

plt.annotate(f'{latency_new[0]}ms', xy=(0, 210), xytext=(-3, 210), color='red')
# plt.annotate(f'{lag_read_cdc_mean[len(lag_read_cdc_mean)-1]:.2f}s', xy=(120, 1), xytext=(125, 180), color='red')


plt.title('Scan Latency and Delta Size\n(Base table size: 100w)', fontsize = 15)
plt.xlabel('Delta Size(w)', fontsize = 20)
plt.ylabel('Scan Latency (ms)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
