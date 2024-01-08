import pandas as pd
import matplotlib.pyplot as plt

df1 = pd.read_csv('3ld_disable_latency_no_delta.csv')
df2 = pd.read_csv('3ld_latency_no_delta.csv')

base_size = df1['Base Table Size'] / 10000
latency1 = df1['Scan Latency'] * 1000
latency2 = df2['Scan Latency'] * 1000

# x start at 5
plt.xticks(range(0, 280, 20))
plt.yticks(range(0, 600, 25))

plt.plot(base_size, latency1, label='Latency without SCD', color='green')

plt.plot(base_size, latency2, label='Latency with SCD', color='red')

plt.title('Scan Latency and Base Table Size\n(Empty Delta Table)', fontsize = 15)
plt.xlabel('Base Table Size(w)', fontsize = 20)
plt.ylabel('Scan Latency (ms)', fontsize = 20)

plt.legend()

plt.savefig('lag_time_plot.png')

plt.show()
