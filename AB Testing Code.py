####A/B Testing Code
import numpy as np
from scipy.stats import stats, shapiro

####Testing for Normality####

x = np.array([148, 154, 158, 160, 161, 162, 166, 170, 182, 195, 236])
#res = stats.shapiro(x)
#print(res.statistic)

test_stat, pvalue = shapiro(x)
print('Test Stat = %.4f, p-value = %.4f' % (test_stat, pvalue))
