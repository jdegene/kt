# -*- coding: utf-8 -*-

# handles all model building and executing related things

import numpy as np
import pandas as pd
import xgboost as xgb

import matplotlib.pyplot as plt

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier



# Load main data frame, use EITHER Result_t1goals or Result_goaldiff as Y
ml_df = pd.read_csv("E:/Test/ml.csv", sep=";")


def create_t1goals_model(ml_df):
    # create new dfs with exactly one output variable
    ml_df_g1 = ml_df.drop("Result_goaldiff", axis=1)

    # define X, Y... basically remove Y from X, transform both to arrays
    Y = ml_df_g1["Result_t1goals"].values
    X = ml_df_g1.drop("Result_t1goals", axis=1).values
        
    test_size = 0.1
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=test_size)

    # fit model no training data
    model = DecisionTreeClassifier().fit(X_train, y_train)

    # make predictions for test data
    y_pred = model.predict(X_test)
    predictions = [round(value) for value in y_pred]
    
    # evaluate predictions
    accuracy = accuracy_score(y_test, predictions)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))
    
    return model
    
    #unique, counts = np.unique(y_pred, return_counts=True)
    #pcount = [str(c / len(y_test) )[:5] for c in counts]
    #print(dict(zip(unique, pcount )))


def create_goaldiff_model(ml_df):
    # create new dfs with exactly one output variable
    ml_df_diff = ml_df.drop("Result_t1goals", axis=1)
    
    # define X, Y... basically remove Y from X, transform both to arrays
    Y = ml_df_diff["Result_goaldiff"].values
    X = ml_df_diff.drop("Result_goaldiff", axis=1).values
    
    test_size = 0.1
    X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=test_size)

    # fit model no training data
    model = DecisionTreeClassifier()
    model.fit(X_train, y_train)

    # make predictions for test data
    y_pred = model.predict(X_test)
    predictions = [round(value) for value in y_pred]
    
    # evaluate predictions
    accuracy = accuracy_score(y_test, predictions)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))

    return model

    #unique, counts = np.unique(y_pred, return_counts=True)
    #pcount = [str(c / len(y_test) )[:5] for c in counts]
    #print(dict(zip(unique, pcount )))


def predict_outcome(model, inData):
    return model.predict(inData.reshape(1,-1))[0]










"""
GARBAGE COLLECTION

importances = model.feature_importances_
std = np.std([tree.feature_importances_ for tree in model.estimators_],
             axis=0)

indices = np.argsort(importances)[::-1]
# Plot the feature importances of the forest
plt.figure(figsize=[16,9])
plt.title("Feature importances")
plt.bar(range(X.shape[1]), importances[indices],
       color="r", yerr=std[indices], align="center")
plt.xticks(range(X.shape[1]), indices)
plt.xlim([-1, X.shape[1]])
plt.show()

values = sorted(zip(ml_df_g1.drop("Result_t1goals", axis=1).columns, model.feature_importances_), key=lambda x: x[1] * -1)
print(values)


def modelfit(alg, dtrain, predictors,useTrainCV=True, cv_folds=5, early_stopping_rounds=50):
    
    if useTrainCV:
        xgb_param = alg.get_xgb_params()
        xgtrain = xgb.DMatrix(dtrain[predictors].values, label=dtrain[target].values)
        cvresult = xgb.cv(xgb_param, xgtrain, num_boost_round=alg.get_params()['n_estimators'], nfold=cv_folds,
            metrics='auc', early_stopping_rounds=early_stopping_rounds, show_progress=False)
        alg.set_params(n_estimators=cvresult.shape[0])
    
    #Fit the algorithm on the data
    alg.fit(dtrain[predictors], dtrain['Disbursed'],eval_metric='auc')
        
    #Predict training set:
    dtrain_predictions = alg.predict(dtrain[predictors])
    dtrain_predprob = alg.predict_proba(dtrain[predictors])[:,1]
        
    #Print model report:
    print( "\nModel Report")
    print( "Accuracy : %.4g" % metrics.accuracy_score(dtrain['Disbursed'].values, dtrain_predictions))
    print( "AUC Score (Train): %f" % metrics.roc_auc_score(dtrain['Disbursed'], dtrain_predprob))
                    
    feat_imp = pd.Series(alg.booster().get_fscore()).sort_values(ascending=False)
    feat_imp.plot(kind='bar', title='Feature Importances')
    plt.ylabel('Feature Importance Score')
"""