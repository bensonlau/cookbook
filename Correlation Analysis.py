'''

"""**Running Through Correlation Analysis**"""
'''
import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr
from scipy.signal import correlate
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.stattools import grangercausalitytests
#from lifelines import CoxPHFitter
import warnings
#warnings.filterwarnings(‘ignore’)

# Generate synthetic Apple One user data

np.random.seed(42)
n_users = 2000
n_weeks = 24  # 6 months of data

# Create user cohorts

user_data = []
for user_id in range(n_users):
# User characteristics
  acquisition_channel = np.random.choice(['direct', 'referral', 'promotion'], p=[0.4, 0.3, 0.3])
  device_count = np.random.poisson(2) + 1

# Early engagement patterns (first 4 weeks)
  base_engagement = np.random.normal(50, 15)
  week1_services_used = max(1, np.random.poisson(2))
  week1_total_hours = np.random.exponential(8)
  week1_cross_service_score = min(100, max(0, base_engagement + np.random.normal(0, 10)))

# Generate weekly progression with some users showing decay
  churn_risk = np.random.random()
  survival_weeks = n_weeks if churn_risk < 0.7 else np.random.randint(4, n_weeks)

  weekly_engagement = []
  for week in range(n_weeks):
      if week < survival_weeks:
          # Engagement follows early pattern with some decay
          decay_factor = 0.95 if churn_risk > 0.5 else 0.98
          engagement = base_engagement * (decay_factor ** week) + np.random.normal(0, 5)
          weekly_engagement.append(max(0, engagement))
      else:
          weekly_engagement.append(0)  # User churned

  user_data.append({
      'user_id': user_id,
      'acquisition_channel': acquisition_channel,
      'device_count': device_count,
      'week1_services_used': week1_services_used,
      'week1_total_hours': week1_total_hours,
      'week1_cross_service_score': week1_cross_service_score,
      'survived_weeks': survival_weeks,
      'retained_6months': 1 if survival_weeks >= n_weeks else 0,
      'weekly_engagement': weekly_engagement,
      'avg_engagement_weeks_13_24': np.mean(weekly_engagement[12:24]) if survival_weeks > 12 else 0
  })

  df = pd.DataFrame(user_data)

display(df)

print("=== APPLE ONE SERVICE: NEAR-TERM vs LONG-TERM CORRELATION ANALYSIS ===\n")

# 1. LAGGED CORRELATION ANALYSIS

print("1. LAGGED CORRELATION ANALYSIS")
print("-" * 40)

near_term_metrics = ['week1_services_used', 'week1_total_hours', 'week1_cross_service_score']
long_term_outcomes = ['retained_6months', 'survived_weeks', 'avg_engagement_weeks_13_24']

correlation_results = []

for near_metric in near_term_metrics:
  for long_metric in long_term_outcomes:
    if long_metric == 'retained_6months':
# Point-biserial correlation for binary outcome
      corr, p_value = pearsonr(df[near_metric], df[long_metric])
    else:
      corr, p_value = pearsonr(df[near_metric], df[long_metric])

    correlation_results.append({
        'near_term_metric': near_metric,
        'long_term_outcome': long_metric,
        'correlation': corr,
        'p_value': p_value,
        'significant': p_value < 0.05
    })

    print(f"{near_metric} → {long_metric}:")
    print(f"  Correlation: {corr:.3f} (p={p_value:.3f}) {'***' if p_value < 0.001 else '**' if p_value < 0.01 else '*' if p_value < 0.05 else ''}")


print("\n" + "="*60 + "\n")


'''commentary:

Most of the correlations are very close to 0, with high p-values (greater than 0.05).
This suggests that there is no statistically significant linear relationship
between "week1_services_used", "week1_total_hours", and most of the long-term
outcomes.

The strongest correlation is between "week1_cross_service_score" and
"avg_engagement_weeks_13_24" (Correlation: 0.386, p=0.000). This is a moderate
positive correlation and is
highly statistically significant (indicated by the '***').

This means that
users with a higher cross-service score in their first week tend to have higher
average engagement in weeks 13-24.
In summary, based on this lagged correlation analysis,
the "week1_cross_service_score" appears to be the most promising early indicator
 of long-term engagement, while the other week 1 metrics show little linear
 association with the long-term outcomes.

'''

# 2. CROSS-CORRELATION FUNCTION ANALYSIS

print("2. CROSS-CORRELATION FUNCTION ANALYSIS")
print("-" * 40)

# Create time series data for cross-correlation

def calculate_cross_correlation(df, early_weeks=4):
    """Calculate cross-correlation between early engagement and later outcomes"""
    cross_corr_results = []

    for user_id in df['user_id'].head(100):  # Sample for demonstration
        user_row = df[df['user_id'] == user_id].iloc[0]
        weekly_data = user_row['weekly_engagement']

        if len(weekly_data) >= 12:  # Need sufficient data
            early_engagement = np.mean(weekly_data[:early_weeks])
            later_engagement = weekly_data[early_weeks:12]

            if len(later_engagement) > 0:
                # Calculate correlation at different lags
                for lag in range(min(8, len(later_engagement))):
                    if lag < len(later_engagement):
                        later_subset = later_engagement[lag:]
                        if len(later_subset) > 2:
                            cross_corr_results.append({
                                'lag_weeks': lag,
                                'early_engagement': early_engagement,
                                'later_engagement': np.mean(later_subset)
                            })

    cross_df = pd.DataFrame(cross_corr_results)
    if not cross_df.empty:
        lag_correlations = cross_df.groupby('lag_weeks').apply(
            lambda x: pearsonr(x['early_engagement'], x['later_engagement'])[0]
        )

        print("Cross-correlation by lag (weeks after initial period):")
        for lag, corr in lag_correlations.items():
            print(f"  Lag {lag} weeks: {corr:.3f}")

        optimal_lag = lag_correlations.abs().idxmax()
        print(f"\nOptimal prediction lag: {optimal_lag} weeks (correlation: {lag_correlations[optimal_lag]:.3f})")


calculate_cross_correlation(df)

print("\n" + "="*60 + "\n")

'''commentary

Cross-correlation by lag: This section shows the correlation coefficient
between early engagement and later engagement at different time lags.

A "lag" of 0 weeks means the correlation between average engagement in weeks 1-4
and average engagement in weeks 4-12. A "lag" of 1 week means the correlation
between average engagement in weeks 1-4 and average engagement in weeks 5-13,
and so on.

Lag 0 weeks: 0.806: This indicates a strong positive correlation (0.806) between
 average engagement in the first 4 weeks and average engagement in the
 subsequent weeks (4-12).

Lag 1 week: 0.777, Lag 2 weeks: 0.747, etc.: As the lag increases, the
correlation coefficient decreases. This suggests that the predictive power of
early engagement for later engagement diminishes as the time gap between the
early and later periods widens.

Optimal prediction lag: 0 weeks (correlation: 0.806): This identifies the lag
with the highest absolute correlation coefficient. In this case, the strongest
correlation is observed at a lag of 0 weeks, meaning that the average engagement
 in the first 4 weeks is the best predictor of average engagement in the
 immediately following period (weeks 4-12).
In summary, the cross-correlation analysis confirms that early engagement
in the first 4 weeks is a strong indicator of engagement in the subsequent weeks
, and its predictive power is highest for the period immediately following the
initial 4 weeks.

'''

# 3. PARTIAL CORRELATION ANALYSIS

print("3. PARTIAL CORRELATION ANALYSIS")
print("-" * 40)

from scipy.stats import pearsonr

def partial_correlation(df, x, y, control_vars):
    # Calculate partial correlation controlling for specified variables
    # Create regression residuals
    from sklearn.linear_model import LinearRegression

    # Residualize x with respect to control variables
    X_control = df[control_vars].values
    reg_x = LinearRegression().fit(X_control, df[x])
    residual_x = df[x] - reg_x.predict(X_control)

    # Residualize y with respect to control variables
    reg_y = LinearRegression().fit(X_control, df[y])
    residual_y = df[y] - reg_y.predict(X_control)

    # Correlation of residuals
    return pearsonr(residual_x, residual_y)


control_variables = ['device_count', 'acquisition_channel'] # Added acquisition_channel as a control variable

for control_var in control_variables:
    print(f"\nControlling for {control_var}:")

    # Convert categorical control variable to dummy variables if necessary
    if df[control_var].dtype == 'object':
        control_vars_dummies = pd.get_dummies(df[control_var], prefix=control_var, drop_first=True).columns.tolist()
        temp_df = pd.concat([df, pd.get_dummies(df[control_var], prefix=control_var, drop_first=True)], axis=1)
    else:
        control_vars_dummies = [control_var]
        temp_df = df.copy()


    for near_metric in ['week1_services_used', 'week1_cross_service_score']:
        for long_metric in ['retained_6months', 'survived_weeks', 'avg_engagement_weeks_13_24']: # Added avg_engagement_weeks_13_24
            # Raw correlation
            raw_corr, _ = pearsonr(temp_df[near_metric], temp_df[long_metric])

            # Partial correlation
            partial_corr, partial_p = partial_correlation(temp_df, near_metric, long_metric, control_vars_dummies)

            print(f"{near_metric} → {long_metric}:")
            print(f"  Raw correlation: {raw_corr:.3f}")
            print(f"  Partial correlation: {partial_corr:.3f} (p={partial_p:.3f})")
            print(f"  Change: {partial_corr - raw_corr:.3f}")

print("\n" + "="*60 + "\n")


'''commentary
With 'avg_engagement_weeks_13_24' included, we can see the following:

When controlling for device_count, the partial correlation between
'week1_cross_service_score' and 'avg_engagement_weeks_13_24' is 0.386 with a
p-value of 0.000. This is still a strong and statistically significant positive
correlation, very similar to the raw correlation. This indicates that the
relationship between 'week1_cross_service_score' and later engagement is not
significantly explained by the number of devices a user has.
When controlling for acquisition_channel, the partial correlation between
'week1_cross_service_score' and 'avg_engagement_weeks_13_24' is 0.387 with a
p-value of 0.000. Again, this is a strong and statistically significant positive
 correlation, very similar to the raw correlation. This suggests that the
 relationship between 'week1_cross_service_score' and later engagement is not
 significantly confounded by the user's acquisition channel.

In summary, the partial correlation analysis, even when controlling for
'device_count' and 'acquisition_channel', confirms that
'week1_cross_service_score' has a significant and relatively strong positive
relationship with average engagement in weeks 13-24. The relationships between
'week1_services_used' and the long-term outcomes remain non-significant after
controlling for these variables.

'''

# 4. PREDICTIVE POWER ANALYSIS

print("4. PREDICTIVE POWER ANALYSIS")
print("-" * 40)

from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, mean_squared_error, r2_score

# Prepare features

feature_cols = ['week1_services_used', 'week1_total_hours', 'week1_cross_service_score', 'device_count']
X = df[feature_cols]

# Predict 6-month retention

y_retention = df['retained_6months']
X_train, X_test, y_train, y_test = train_test_split(X, y_retention, test_size=0.3, random_state=42)

rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
rf_classifier.fit(X_train, y_train)

y_pred_proba = rf_classifier.predict_proba(X_test)[:, 1]
auc_score = roc_auc_score(y_test, y_pred_proba)

print("Predicting 6-month retention from Week 1 metrics:")
print(f"  AUC Score: {auc_score:.3f}")

# Feature importance

feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': rf_classifier.feature_importances_
}).sort_values('importance', ascending=False)

print("  Feature Importance:")
for _, row in feature_importance.iterrows():
    print(f"    {row['feature']}: {row['importance']:.3f}")

# Predict survival weeks

y_survival = df['survived_weeks']
X_train, X_test, y_train, y_test = train_test_split(X, y_survival, test_size=0.3, random_state=42)

rf_regressor = RandomForestRegressor(n_estimators=100, random_state=42)
rf_regressor.fit(X_train, y_train)

y_pred = rf_regressor.predict(X_test)
r2 = r2_score(y_test, y_pred)

print(f"\nPredicting survival weeks from Week 1 metrics:")
print(f"  R² Score: {r2:.3f}")

print("\n" + "="*60 + "\n")

'''commentary
Predicting 6-month retention from Week 1 metrics: This section uses a
RandomForestClassifier to predict whether a user will be retained after 6 months
 based on their week 1 metrics and device count.

AUC Score: 0.515: The Area Under the Curve (AUC) is a measure of the
classifier's ability to distinguish between the two classes
(retained vs. not retained). An AUC of 0.5 suggests the model is performing no
better than random guessing. An AUC of 1.0 would indicate a perfect classifier.

In this case, an AUC of 0.515 is very close to 0.5, indicating that these week 1
 metrics are not very good at predicting 6-month retention on their own.

Feature Importance: This shows which of the input features
(week 1 metrics and device count) were most important in the
RandomForestClassifier model for predicting 6-month retention.
week1_total_hours: 0.410 and week1_cross_service_score: 0.401 have the highest
importance scores, suggesting that these two metrics contributed the most to the
 model's prediction, although the overall predictive power is low as indicated
 by the AUC score.
device_count: 0.101 and week1_services_used: 0.088 have lower importance scores.

Predicting survival weeks from Week 1 metrics: This section uses a
RandomForestRegressor to predict the number of weeks a user will survive based
on their week 1 metrics and device count.
R² Score: -0.085: The R-squared (R²) score is a measure of how well the
regression model fits the data. It represents the proportion of the variance in
the dependent variable (survival weeks) that is predictable from the independent
 variables (week 1 metrics and device count). An R² score can range from 0 to 1
  (or even be negative in some cases, as here). A negative R² score indicates
  that the model fits the data worse than a horizontal line at the mean of the
   dependent variable. This means that these week 1 metrics are not effective in
    predicting the number of weeks a user will survive.

In summary, the predictive power analysis suggests that while week1_total_hours
and week1_cross_service_score are the most influential of the tested features
for predicting 6-month retention, the overall predictive power of these week 1
metrics for both 6-month retention (AUC of 0.515) and survival weeks
(negative R²) is very low in this synthetic dataset.

'''

# 5. ACQUISITION COHORT-BASED VALIDATION

print("5. COHORT-BASED VALIDATION")
print("-" * 40)

# Simulate different acquisition cohorts

df['cohort'] = np.random.choice(['Jan', 'Feb', 'Mar', 'Apr'], size=len(df))

print("Correlation stability across acquisition cohorts:")
for cohort in df['cohort'].unique():
    cohort_df = df[df['cohort'] == cohort]
    corr, p_val = pearsonr(cohort_df['week1_cross_service_score'], cohort_df['retained_6months'])
    print(f"  {cohort} cohort: {corr:.3f} (n={len(cohort_df)}) {'*' if p_val < 0.05 else ''}")

# Overall correlation

overall_corr, overall_p = pearsonr(df['week1_cross_service_score'], df['retained_6months'])
print(f"  Overall: {overall_corr:.3f}")

print("\n" + "="*60 + "\n")


'''commentary
Correlation stability across acquisition cohorts: This line introduces the
analysis, which examines if the correlation between week1_cross_service_score
and retained_6months is similar for users who joined in January, February,
March, and April.

Jan cohort: -0.000 (n=494): For users acquired in January, the correlation
between week1_cross_service_score and retained_6months is very close to zero
(-0.000). The n=494 indicates the number of users in this cohort.
There is no asterisk, meaning the correlation is not statistically significant
(p >= 0.05).

Apr cohort: -0.040 (n=527): For the April cohort, the correlation is also weak
and negative (-0.040), and not statistically significant.

Mar cohort: 0.009 (n=479): The March cohort shows a very weak positive
correlation (0.009), also not statistically significant.

Feb cohort: 0.021 (n=500): The February cohort has a weak positive correlation
(0.021), which is also not statistically significant.

Overall: -0.004: This is the overall correlation between
week1_cross_service_score and retained_6months across all cohorts, which is also
 very close to zero and not statistically significant (as seen in the initial
 lagged correlation analysis).
In summary, the cohort-based validation shows that in this synthetic dataset,
the correlation between week1_cross_service_score and retained_6months is c
onsistently weak and not statistically significant across all the simulated
acquisition cohorts.

This suggests that week1_cross_service_score is not a reliable predictor of
6-month retention, and its relationship with retention doesn't appear to vary
much depending on the acquisition month.


'''

# 6. KEY INSIGHTS SUMMARY

print("6. KEY INSIGHTS SUMMARY")
print("-" * 40)

# Find strongest predictors

best_correlations = pd.DataFrame(correlation_results)
best_predictors = best_correlations[best_correlations['significant'] == True].sort_values('correlation', key=abs, ascending=False)

print("Strongest Near-Term Predictors of Long-Term Success:")
for _, row in best_predictors.head(3).iterrows():
    print(f"  {row['near_term_metric']} → {row['long_term_outcome']}")
    print(f"    Correlation: {row['correlation']:.3f}")

    if abs(row['correlation']) > 0.3: # Use absolute value for strength
        strength = "Strong"
    elif abs(row['correlation']) > 0.1: # Use absolute value for strength
        strength = "Moderate"
    else:
        strength = "Weak"
    print(f"    Predictive strength: {strength}")


print(f"\nRecommendation: Focus on tracking {best_predictors.iloc[0]['near_term_metric']}")
print("as your primary early indicator for long-term success.")

# Calculate practical thresholds - Quantile Analysis

best_near_term = best_predictors.iloc[0]['near_term_metric']

print(f"\nActionable Thresholds for {best_near_term} (Quantile Analysis):")

# Calculate quartiles
quartiles = df[best_near_term].quantile([0.25, 0.5, 0.75])
print(f"  Quartile thresholds: {quartiles.to_dict()}")

# Group by quartiles and calculate retention
df['score_quartile'] = pd.qcut(df[best_near_term], q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop') # Added duplicates='drop'
retention_by_quartile = df.groupby('score_quartile')['retained_6months'].agg(['mean', 'count'])

print("\n  6-month retention by quartile:")
print(retention_by_quartile)

'''commentary:
Strongest Near-Term Predictors of Long-Term Success: This identifies which of
the initial metrics had the most statistically significant correlations with the
 long-term outcomes based on the lagged correlation analysis (Section 1).

week1_cross_service_score → avg_engagement_weeks_13_24: This line highlights the
 strongest statistically significant relationship found. The
 week1_cross_service_score is identified as a predictor for
 avg_engagement_weeks_13_24.

Correlation: 0.386: This is the Pearson correlation coefficient between these
two variables, indicating a moderate positive linear relationship.

Predictive strength: Strong: Based on the correlation value
(0.386 is greater than 0.3), the predictive strength is categorized as "Strong".

Recommendation: This provides a practical suggestion based on the findings.

Focus on tracking week1_cross_service_score as your primary early indicator for
 long-term success: Since week1_cross_service_score showed the strongest and
 most statistically significant correlation with later engagement
 (avg_engagement_weeks_13_24), the recommendation is to prioritize tracking this
  metric as an early indicator of user long-term engagement.

Actionable Thresholds for week1_cross_service_score (Quantile Analysis): This
section now uses quantile analysis to provide more nuanced thresholds.

Quartile thresholds: {'0.25': 31.5, '0.5': 49.5, '0.75': 67.5}: These are the
actual score values that mark the boundaries of the quartiles. Users with scores
 up to 31.5 are in Q1, between 31.5 and 49.5 are in Q2, between 49.5 and 67.5 are
  in Q3, and above 67.5 are in Q4.

6-month retention by quartile: This table shows the average 6-month retention
rate and the number of users ('count') within each quartile.
score_quartile  mean  count
Q1             0.694    500
Q2             0.694    500
Q3             0.704    500
Q4             0.708    500

Q1 (lowest scores): 69.4% retention.
Q2: 69.4% retention.
Q3: 70.4% retention.
Q4 (highest scores): 70.8% retention.

In summary, the quantile analysis provides a more detailed view than just the
median split. While the differences in retention rates between quartiles are
still relatively small in this synthetic dataset, there is a slight trend
towards higher retention in the upper quartiles (Q3 and Q4) compared to the
lower ones (Q1 and Q2). This suggests that users with higher
week1_cross_service_score are slightly more likely to be retained. These quartile
values (31.5, 49.5, 67.5) can serve as more granular practical thresholds for
segmenting users based on their early cross-service engagement.

'''

# 7. CLUSTERING ANALYSIS FOR THRESHOLDS

print("7. CLUSTERING ANALYSIS FOR THRESHOLDS")
print("-" * 40)

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Select early engagement features for clustering
clustering_features = ['week1_services_used', 'week1_total_hours', 'week1_cross_service_score']
X_clustering = df[clustering_features]

# Standardize the features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_clustering)

# Determine the optimal number of clusters (Elbow method - for demonstration, we'll choose a small number)
# In a real analysis, you would typically use methods like the Elbow method or Silhouette score
# to help determine the optimal number of clusters. Let's start with 3 clusters.
n_clusters = 3
kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10) # Added n_init
df['cluster'] = kmeans.fit_predict(X_scaled)

# Analyze long-term outcomes by cluster
print(f"\nAnalyzing long-term outcomes by {n_clusters} clusters:")

# 6-month retention by cluster
retention_by_cluster = df.groupby('cluster')['retained_6months'].agg(['mean', 'count'])
print("\n  6-month retention by cluster:")
print(retention_by_cluster)

# Average survival weeks by cluster
survival_by_cluster = df.groupby('cluster')['survived_weeks'].agg(['mean', 'count'])
print("\n  Average survival weeks by cluster:")
print(survival_by_cluster)

# Average engagement weeks 13-24 by cluster
avg_engagement_by_cluster = df.groupby('cluster')['avg_engagement_weeks_13_24'].agg(['mean', 'count'])
print("\n  Average engagement weeks 13-24 by cluster:")
print(avg_engagement_by_cluster)

# Describe the characteristics of each cluster (optional, but helpful)
print("\n  Cluster characteristics (mean of scaled features):")
print(pd.DataFrame(X_scaled, columns=clustering_features).groupby(df['cluster']).mean())

print("\n" + "="*60 + "\n")