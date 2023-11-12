from IPython.display import display
from pandas_dq import dq_report
from streamlit_folium import st_folium, folium_static

import streamlit as st
import matplotlib.pyplot as plt
import numpy as np  # linear algebra
import os
import pandas as pd  # data processing, CSV file I/O (e.g. load_csv)
import seaborn as sns
import dask.dataframe as dd
import requests
import folium


@st.cache_data
def load_csv(urlku):
    df_csv = pd.read_csv(urlku)
    return df_csv


dir_fold = "E-Commerce Public Dataset"

seller_df = load_csv(f"{dir_fold}/sellers_dataset.csv")
order_df = load_csv(f"{dir_fold}/orders_dataset.csv")
geolocate_df = load_csv(f"{dir_fold}/geolocation_dataset.csv")
cust_df = load_csv(f"{dir_fold}/customers_dataset.csv")
order_item_df = load_csv(f"{dir_fold}/order_items_dataset.csv")
order_review_df = load_csv(f"{dir_fold}/order_reviews_dataset.csv")
prod_df = load_csv(f"{dir_fold}/products_dataset.csv")
prod_cat_df = load_csv(f"{dir_fold}/product_category_name_translation.csv")
order_pay_df = load_csv(f"{dir_fold}/order_payments_dataset.csv")

print("seller_df")
# display(seller_df)
print("geolocate_df")
# display(geolocate_df)
print("cust_df")
# display(cust_df)

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:14.689201Z","iopub.execute_input":"2023-11-11T02:52:14.689765Z","iopub.status.idle":"2023-11-11T02:52:14.726888Z","shell.execute_reply.started":"2023-11-11T02:52:14.689730Z","shell.execute_reply":"2023-11-11T02:52:14.725523Z"}}
print("prod_df")
# display(prod_df)
print("prod_cat_df")
# display(prod_cat_df)

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:14.728696Z","iopub.execute_input":"2023-11-11T02:52:14.729138Z","iopub.status.idle":"2023-11-11T02:52:14.796874Z","shell.execute_reply.started":"2023-11-11T02:52:14.729085Z","shell.execute_reply":"2023-11-11T02:52:14.795522Z"}}
print("order_df")
# display(order_df)
print("order_item_df")
# display(order_item_df)
print("order_review_df")
# display(order_review_df)
print("order_pay_df")
# display(order_pay_df)

# %% [markdown]
# ### Assessing data

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Seller DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:14.798912Z","iopub.execute_input":"2023-11-11T02:52:14.800415Z","iopub.status.idle":"2023-11-11T02:52:14.946930Z","shell.execute_reply.started":"2023-11-11T02:52:14.800364Z","shell.execute_reply":"2023-11-11T02:52:14.945535Z"}}
seller_df["seller_zip_code_prefix"] = seller_df["seller_zip_code_prefix"].astype(
    "object"
)
# dq_r = dq_report(seller_df, verbose=1)

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Geolocate DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:14.948664Z","iopub.execute_input":"2023-11-11T02:52:14.949098Z","iopub.status.idle":"2023-11-11T02:52:19.005807Z","shell.execute_reply.started":"2023-11-11T02:52:14.949058Z","shell.execute_reply":"2023-11-11T02:52:19.004493Z"}}
geolocate_df["geolocation_zip_code_prefix"] = geolocate_df[
    "geolocation_zip_code_prefix"
].astype("object")
# dq_r = dq_report(geolocate_df, verbose=1)

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Customer DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:19.007718Z","iopub.execute_input":"2023-11-11T02:52:19.008137Z","iopub.status.idle":"2023-11-11T02:52:20.069920Z","shell.execute_reply.started":"2023-11-11T02:52:19.008102Z","shell.execute_reply":"2023-11-11T02:52:20.068526Z"}}
cust_df["customer_zip_code_prefix"] = cust_df["customer_zip_code_prefix"].astype(
    "object"
)
# dq_r = dq_report(cust_df, verbose=1)

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Product DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:20.071690Z","iopub.execute_input":"2023-11-11T02:52:20.072092Z","iopub.status.idle":"2023-11-11T02:52:20.468605Z","shell.execute_reply.started":"2023-11-11T02:52:20.072061Z","shell.execute_reply":"2023-11-11T02:52:20.467745Z"}}
# dq_r = dq_report(prod_df, verbose=1)

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Order DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:20.472274Z","iopub.execute_input":"2023-11-11T02:52:20.472865Z","iopub.status.idle":"2023-11-11T02:52:23.418818Z","shell.execute_reply.started":"2023-11-11T02:52:20.472831Z","shell.execute_reply":"2023-11-11T02:52:23.417669Z"}}
order_df.order_purchase_timestamp = pd.to_datetime(order_df.order_purchase_timestamp)
order_df.order_approved_at = pd.to_datetime(order_df.order_approved_at)
order_df.order_delivered_carrier_date = pd.to_datetime(
    order_df.order_delivered_carrier_date
)
order_df.order_delivered_customer_date = pd.to_datetime(
    order_df.order_delivered_customer_date
)
order_df.order_estimated_delivery_date = pd.to_datetime(
    order_df.order_estimated_delivery_date
)

# dq_r = dq_report(order_df, verbose=1)

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Order Item DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:23.420597Z","iopub.execute_input":"2023-11-11T02:52:23.421253Z","iopub.status.idle":"2023-11-11T02:52:24.662123Z","shell.execute_reply.started":"2023-11-11T02:52:23.421216Z","shell.execute_reply":"2023-11-11T02:52:24.660857Z"}}
order_item_df.shipping_limit_date = pd.to_datetime(order_item_df.shipping_limit_date)
# dq_r = dq_report(order_item_df, verbose=1)
# dq_r

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Order Review DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:24.663988Z","iopub.execute_input":"2023-11-11T02:52:24.664371Z","iopub.status.idle":"2023-11-11T02:52:26.392065Z","shell.execute_reply.started":"2023-11-11T02:52:24.664338Z","shell.execute_reply":"2023-11-11T02:52:26.391156Z"}}
order_review_df.review_creation_date = pd.to_datetime(
    order_review_df.review_creation_date
)
order_review_df.review_answer_timestamp = pd.to_datetime(
    order_review_df.review_answer_timestamp
)

# dq_r = dq_report(order_review_df, verbose=1)
# dq_r

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Order Payment DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:26.393321Z","iopub.execute_input":"2023-11-11T02:52:26.394259Z","iopub.status.idle":"2023-11-11T02:52:26.873296Z","shell.execute_reply.started":"2023-11-11T02:52:26.394223Z","shell.execute_reply":"2023-11-11T02:52:26.872265Z"}}
# dq_r = dq_report(order_pay_df, verbose=1)
# dq_r

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Kesimpulan
# * Product Df **memiliki Missing Value**
# * Order Df **memiliki Missing Value**
# * Order Review Df **memiliki Missing Value**

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:26.874761Z","iopub.execute_input":"2023-11-11T02:52:26.875197Z","iopub.status.idle":"2023-11-11T02:52:26.884042Z","shell.execute_reply.started":"2023-11-11T02:52:26.875159Z","shell.execute_reply":"2023-11-11T02:52:26.882453Z"}}
# print(f"seller_df       -> {seller_df.shape}")
# print(f"cust_df         -> {cust_df.shape}")
# print(f"geolocate_df    -> {geolocate_df.shape}")
# print()
# print(f"prod_df         -> {prod_df.shape}")
# print(f"prod_cat_df     -> {prod_cat_df.shape}")
# print()
# print(f"order_df        -> {order_df.shape}")
# print(f"order_item_df   -> {order_item_df.shape}")
# print(f"order_review_df -> {order_review_df.shape}")
# print(f"order_pay_df    -> {order_pay_df.shape}")

# %% [markdown]
# ### Cleaning Data

# %% [markdown]
# #### Duplicated geolocate

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:26.886073Z","iopub.execute_input":"2023-11-11T02:52:26.886577Z","iopub.status.idle":"2023-11-11T02:52:27.545083Z","shell.execute_reply.started":"2023-11-11T02:52:26.886537Z","shell.execute_reply":"2023-11-11T02:52:27.543989Z"}}
geolocate_df = geolocate_df.drop_duplicates()
geolocate_df = geolocate_df.drop_duplicates(["geolocation_zip_code_prefix"])
geolocate_df = geolocate_df.reset_index(drop=True)
# geolocate_df

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Cleaning Product DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.546610Z","iopub.execute_input":"2023-11-11T02:52:27.547203Z","iopub.status.idle":"2023-11-11T02:52:27.683113Z","shell.execute_reply.started":"2023-11-11T02:52:27.547169Z","shell.execute_reply":"2023-11-11T02:52:27.681758Z"}}
prod_df["product_category_name"] = prod_df["product_category_name"].fillna(
    prod_df["product_category_name"].mode()[0]
)
# Delete that columns because name,description, and photo is doesn't matter
prod_df = prod_df.drop(
    [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
    ],
    axis=1,
)
prod_df["product_weight_g"] = prod_df["product_weight_g"].fillna(
    prod_df["product_weight_g"].median()
)
prod_df["product_length_cm"] = prod_df["product_length_cm"].fillna(
    prod_df["product_length_cm"].median()
)
prod_df["product_height_cm"] = prod_df["product_height_cm"].fillna(
    prod_df["product_height_cm"].median()
)
prod_df["product_width_cm"] = prod_df["product_width_cm"].fillna(
    prod_df["product_width_cm"].median()
)

dict_trans = {}
for index, val in prod_cat_df.iterrows():
    dict_trans[val["product_category_name"]] = val["product_category_name_english"]
prod_df.product_category_name = prod_df.product_category_name.replace(dict_trans)
# prod_df.isna().sum()

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Cleaning Order DataFrame

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.685902Z","iopub.execute_input":"2023-11-11T02:52:27.686908Z","iopub.status.idle":"2023-11-11T02:52:27.725187Z","shell.execute_reply.started":"2023-11-11T02:52:27.686870Z","shell.execute_reply":"2023-11-11T02:52:27.723797Z"}}
# display(order_df[order_df.order_status == "created"].isna().sum())
# order_df[order_df.order_status == "created"]

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.726770Z","iopub.execute_input":"2023-11-11T02:52:27.727148Z","iopub.status.idle":"2023-11-11T02:52:27.765878Z","shell.execute_reply.started":"2023-11-11T02:52:27.727119Z","shell.execute_reply":"2023-11-11T02:52:27.764454Z"}}
# display(order_df[order_df.order_status == "approved"].isna().sum())
# order_df[order_df.order_status == "approved"]

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.767898Z","iopub.execute_input":"2023-11-11T02:52:27.768340Z","iopub.status.idle":"2023-11-11T02:52:27.806202Z","shell.execute_reply.started":"2023-11-11T02:52:27.768308Z","shell.execute_reply":"2023-11-11T02:52:27.804837Z"}}
# display(order_df[order_df.order_status == "processing"].isna().sum())
# order_df[order_df.order_status == "processing"].head()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.807399Z","iopub.execute_input":"2023-11-11T02:52:27.807818Z","iopub.status.idle":"2023-11-11T02:52:27.845962Z","shell.execute_reply.started":"2023-11-11T02:52:27.807780Z","shell.execute_reply":"2023-11-11T02:52:27.844699Z"}}
# display(order_df[order_df.order_status == "invoiced"].isna().sum())
# order_df[order_df.order_status == "invoiced"].head()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.847995Z","iopub.execute_input":"2023-11-11T02:52:27.848798Z","iopub.status.idle":"2023-11-11T02:52:27.896282Z","shell.execute_reply.started":"2023-11-11T02:52:27.848752Z","shell.execute_reply":"2023-11-11T02:52:27.895157Z"}}
# display(order_df[order_df.order_status == "shipped"].isna().sum())
# order_df[order_df.order_status == "shipped"].head()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.899085Z","iopub.execute_input":"2023-11-11T02:52:27.899477Z","iopub.status.idle":"2023-11-11T02:52:27.977138Z","shell.execute_reply.started":"2023-11-11T02:52:27.899448Z","shell.execute_reply":"2023-11-11T02:52:27.975769Z"}}
# display(order_df[order_df.order_status == "delivered"].isna().sum())
# order_df[order_df.order_status == "delivered"].head()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:27.979159Z","iopub.execute_input":"2023-11-11T02:52:27.979532Z","iopub.status.idle":"2023-11-11T02:52:28.006774Z","shell.execute_reply.started":"2023-11-11T02:52:27.979502Z","shell.execute_reply":"2023-11-11T02:52:28.004999Z"}}
# order_df[order_df.order_status == "canceled"].head()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:28.008432Z","iopub.execute_input":"2023-11-11T02:52:28.008806Z","iopub.status.idle":"2023-11-11T02:52:28.036232Z","shell.execute_reply.started":"2023-11-11T02:52:28.008777Z","shell.execute_reply":"2023-11-11T02:52:28.034831Z"}}
# order_df[order_df.order_status == "unavailable"].head()

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Cleaning Order Review

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:28.037875Z","iopub.execute_input":"2023-11-11T02:52:28.038275Z","iopub.status.idle":"2023-11-11T02:52:28.072770Z","shell.execute_reply.started":"2023-11-11T02:52:28.038245Z","shell.execute_reply":"2023-11-11T02:52:28.071382Z"}}
order_review_df = order_review_df.drop(
    [
        "review_comment_title",
        "review_comment_message",
    ],
    axis=1,
)
# order_review_df

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Merge Order

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:28.074208Z","iopub.execute_input":"2023-11-11T02:52:28.074550Z","iopub.status.idle":"2023-11-11T02:52:28.422088Z","shell.execute_reply.started":"2023-11-11T02:52:28.074521Z","shell.execute_reply":"2023-11-11T02:52:28.420562Z"}}
ord_ord_it_df = pd.merge(order_df, order_item_df, how="left", on="order_id")
ord_ord_it_df = ord_ord_it_df[~ord_ord_it_df.duplicated()]
ord_ord_it_df.isna().sum()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:28.423371Z","iopub.execute_input":"2023-11-11T02:52:28.423691Z","iopub.status.idle":"2023-11-11T02:52:28.688426Z","shell.execute_reply.started":"2023-11-11T02:52:28.423667Z","shell.execute_reply":"2023-11-11T02:52:28.687078Z"}}
order_rev_df = order_review_df.groupby(["order_id"]).agg({"review_score": "mean"})
order_rev_df = order_rev_df.reset_index()
order_rev_df = order_rev_df[~order_rev_df.duplicated()]
# order_rev_df

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:28.696713Z","iopub.execute_input":"2023-11-11T02:52:28.697148Z","iopub.status.idle":"2023-11-11T02:52:29.120554Z","shell.execute_reply.started":"2023-11-11T02:52:28.697114Z","shell.execute_reply":"2023-11-11T02:52:29.119645Z"}}
ord_ord_it_rev_df = pd.merge(ord_ord_it_df, order_rev_df, how="left", on="order_id")
ord_ord_it_rev_df = ord_ord_it_rev_df[~ord_ord_it_rev_df.duplicated()]
# ord_ord_it_rev_df.isna().sum()
# ord_ord_it_rev_df

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:29.121952Z","iopub.execute_input":"2023-11-11T02:52:29.122467Z","iopub.status.idle":"2023-11-11T02:52:29.602132Z","shell.execute_reply.started":"2023-11-11T02:52:29.122436Z","shell.execute_reply":"2023-11-11T02:52:29.600969Z"}}
order_master_df = pd.merge(ord_ord_it_rev_df, order_pay_df, on="order_id", how="left")
order_master_df = order_master_df[~order_master_df.duplicated()]
# order_master_df

# %% [markdown] {"jp-MarkdownHeadingCollapsed":true}
# #### Merge Other

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:29.604109Z","iopub.execute_input":"2023-11-11T02:52:29.604559Z","iopub.status.idle":"2023-11-11T02:52:29.647002Z","shell.execute_reply.started":"2023-11-11T02:52:29.604514Z","shell.execute_reply":"2023-11-11T02:52:29.645723Z"}}
seller_geo_df = pd.merge(
    seller_df,
    geolocate_df,
    how="inner",
    left_on="seller_zip_code_prefix",
    right_on="geolocation_zip_code_prefix",
)
# #display(seller_df)
# #display(geolocate_df)
# display(seller_geo_df.shape)
seller_geo_df = seller_geo_df.drop(
    [
        "geolocation_zip_code_prefix",
        "geolocation_city",
        "geolocation_state",
    ],
    axis=1,
)
seller_geo_df = seller_geo_df.rename(
    {
        "geolocation_lat": "seller_lat",
        "geolocation_lng": "seller_lng",
    },
    axis=1,
)
seller_geo_df = seller_geo_df[~seller_geo_df.duplicated()]
seller_geo_df = seller_geo_df.reset_index(drop=True)
seller_geo_df = seller_geo_df[~seller_geo_df.duplicated()]
# seller_geo_df

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:29.648586Z","iopub.execute_input":"2023-11-11T02:52:29.648998Z","iopub.status.idle":"2023-11-11T02:52:29.887498Z","shell.execute_reply.started":"2023-11-11T02:52:29.648967Z","shell.execute_reply":"2023-11-11T02:52:29.886028Z"}}
cust_geo_df = pd.merge(
    cust_df,
    geolocate_df,
    how="left",
    left_on="customer_zip_code_prefix",
    right_on="geolocation_zip_code_prefix",
)
cust_geo_df = cust_geo_df.drop(
    [
        "geolocation_zip_code_prefix",
        "geolocation_city",
        "geolocation_state",
    ],
    axis=1,
)
cust_geo_df = cust_geo_df.rename(
    {
        "geolocation_lat": "customer_lat",
        "geolocation_lng": "customer_lng",
    },
    axis=1,
)
cust_geo_df = cust_geo_df[~cust_geo_df.duplicated()]
cust_geo_df = cust_geo_df.reset_index(drop=True)
# cust_geo_df

# %% [markdown]
# #### Merge All

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:29.890040Z","iopub.execute_input":"2023-11-11T02:52:29.890523Z","iopub.status.idle":"2023-11-11T02:52:32.835178Z","shell.execute_reply.started":"2023-11-11T02:52:29.890481Z","shell.execute_reply":"2023-11-11T02:52:32.834030Z"}}
order_item_mer_df = pd.merge(order_df, order_item_df, how="right", on="order_id")
order_item_mer_df = order_item_mer_df.sort_values(
    [
        "product_id",
        "order_id",
        "customer_id",
    ]
)
ord_ord_item_cusor_df = order_item_mer_df.groupby(
    ["customer_id", "order_id", "product_id"]
).agg({"order_item_id": "count", "price": "median"})
ord_ord_item_cusor_df = ord_ord_item_cusor_df.reset_index()
ord_ord_item_cusor_df = ord_ord_item_cusor_df.sort_values(
    ["customer_id", "order_id", "product_id"]
)
ord_ord_item_cusor_df = ord_ord_item_cusor_df.set_index(
    ["customer_id", "order_id", "product_id"]
)
# #display(ord_ord_item_cusor_df[ord_ord_item_cusor_df.index.get_level_values(1) == '8272b63d03f5f79c56e9e4120aec44ef'])
# #display(order_item_mer_df.query('customer_id=="bd5d39761aa56689a265d95d8d32b8be" & order_id=="ab14fdcfbe524636d65ee38360e22ce8" & product_id=="9571759451b1d780ee7c15012ea109d4"'))
ord_ord_item_cusor_df = ord_ord_item_cusor_df.reset_index()
# display(ord_ord_item_cusor_df)

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:32.837299Z","iopub.execute_input":"2023-11-11T02:52:32.837779Z","iopub.status.idle":"2023-11-11T02:52:32.944882Z","shell.execute_reply.started":"2023-11-11T02:52:32.837734Z","shell.execute_reply":"2023-11-11T02:52:32.943481Z"}}
total_spend_product = (
    ord_ord_item_cusor_df.groupby(
        "product_id",
    )
    .agg({"price": "sum"})
    .sort_values("price", ascending=False)
)
total_spend_product = total_spend_product.reset_index()
# total_spend_product = total_spend_product.merge(prod_df,on='product_id',how='left')
# total_spend_product = total_order_product[['product_id','order_item_id','product_category_name']]
# total_order_product = total_order_product.groupby('product_category_name').agg({'order_item_id':'sum'})
# total_order_product = total_order_product.reset_index()
# total_order_product = total_order_product.sort_values('order_item_id',ascending=False).reset_index(drop=True)
# display(total_spend_product)

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:32.946620Z","iopub.execute_input":"2023-11-11T02:52:32.947153Z","iopub.status.idle":"2023-11-11T02:52:33.119003Z","shell.execute_reply.started":"2023-11-11T02:52:32.947112Z","shell.execute_reply":"2023-11-11T02:52:33.117680Z"}}
total_order_product = (
    ord_ord_item_cusor_df.groupby(
        "product_id",
    )
    .agg({"order_item_id": "sum"})
    .sort_values("order_item_id", ascending=False)
)
total_order_product = total_order_product.reset_index()
total_order_product = total_order_product.merge(prod_df, on="product_id", how="left")
total_order_product = total_order_product[
    ["product_id", "order_item_id", "product_category_name"]
]
total_order_product = total_order_product.groupby("product_category_name").agg(
    {"order_item_id": "sum"}
)
total_order_product = total_order_product.reset_index()
total_order_product = total_order_product.sort_values(
    "order_item_id", ascending=False
).reset_index(drop=True)
# display(total_order_product)

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:33.120396Z","iopub.execute_input":"2023-11-11T02:52:33.120743Z","iopub.status.idle":"2023-11-11T02:52:33.759958Z","shell.execute_reply.started":"2023-11-11T02:52:33.120714Z","shell.execute_reply":"2023-11-11T02:52:33.758654Z"}}
harga_df = order_master_df[
    [
        "order_id",
        "customer_id",
        "seller_id",
        "product_id",
        "order_item_id",
        "price",
        "freight_value",
    ]
]
# display(harga_df.shape)
harga_df = harga_df[~harga_df.duplicated()]
# harga_df = harga_df.groupby(['order_id',
#                              'customer_id']
#                            ).agg({'price':'sum',
#                                   'freight_value':'sum'
#                                  }
#                                 )
# harga_df = harga_df.reset_index()
cust_ord_df = (
    harga_df.groupby(["customer_id"]).agg({"order_id": "count"}).sort_values("order_id")
)
cust_ord_df = cust_ord_df.reset_index()
cust_ord_df = pd.merge(cust_ord_df, cust_geo_df, on="customer_id", how="left")
# display(cust_ord_df)
cust_ord_state_df = (
    cust_ord_df.groupby(["customer_state"])
    .agg({"order_id": "sum"})
    .sort_values("order_id", ascending=False)
)
cust_ord_state_df = cust_ord_state_df.reset_index()
# cust_ord_state_df

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:33.761331Z","iopub.execute_input":"2023-11-11T02:52:33.761658Z","iopub.status.idle":"2023-11-11T02:52:34.152102Z","shell.execute_reply.started":"2023-11-11T02:52:33.761631Z","shell.execute_reply":"2023-11-11T02:52:34.151184Z"}}
harga_df = order_master_df[
    [
        "order_id",
        "customer_id",
        "seller_id",
        "product_id",
        "order_item_id",
        "price",
        "freight_value",
    ]
]
# display(harga_df.shape)
harga_df = harga_df[~harga_df.duplicated()]

seller_prod_df = harga_df[["seller_id", "product_id"]]
# #display(seller_prod_df.shape)
seller_prod_df = pd.merge(seller_prod_df, seller_geo_df, on="seller_id", how="left")
# #display(seller_prod_df.shape)
seller_prod_df = pd.merge(seller_prod_df, prod_df, on="product_id", how="left")
# #display(seller_prod_df.shape)
seller_prod_df = seller_prod_df[
    ["seller_id", "product_id", "seller_state", "product_category_name"]
]
seller_prod_cnt_df = seller_prod_df.groupby(["seller_id"]).agg({"product_id": "count"})
seller_prod_cnt_df = seller_prod_cnt_df.reset_index()
# display(seller_prod_cnt_df.shape)
# #display(seller_prod_cnt_df)
seller_prod_cnt_df = pd.merge(
    seller_prod_cnt_df, seller_prod_df, how="inner", on="seller_id"
)
seller_prod_cnt_df = seller_prod_cnt_df[["seller_id", "product_id_x", "seller_state"]]
seller_prod_cnt_df = seller_prod_cnt_df.drop_duplicates()
seller_prod_cnt_df = seller_prod_cnt_df.sort_values("product_id_x")
seller_prod_cnt_df = seller_prod_cnt_df.rename({"product_id_x": "product_id"}, axis=1)
seller_prod_cnt_df = pd.merge(
    seller_prod_cnt_df, seller_geo_df, on="seller_id", how="left"
)
seller_prod_cnt_df = seller_prod_cnt_df[
    ["seller_id", "product_id", "seller_state_x", "seller_lat", "seller_lng"]
]
seller_prod_cnt_df = seller_prod_cnt_df.rename(
    {"seller_state_x": "seller_state"}, axis=1
)
# seller_prod_cnt_df

# %% [markdown]
# ## Exploratory Data Analysis (EDA)

# %% [markdown]
# ### Explore Data Order

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:34.153241Z","iopub.execute_input":"2023-11-11T02:52:34.154406Z","iopub.status.idle":"2023-11-11T02:52:34.263481Z","shell.execute_reply.started":"2023-11-11T02:52:34.154369Z","shell.execute_reply":"2023-11-11T02:52:34.262193Z"}}
# display(order_master_df.shape)
# order_master_df.describe().T

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:34.264738Z","iopub.execute_input":"2023-11-11T02:52:34.265066Z","iopub.status.idle":"2023-11-11T02:52:34.349737Z","shell.execute_reply.started":"2023-11-11T02:52:34.265035Z","shell.execute_reply":"2023-11-11T02:52:34.348762Z"}}
# display(cust_geo_df.describe().T)
# display(seller_geo_df.describe().T)
# display(prod_df.describe().T)

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:34.351624Z","iopub.execute_input":"2023-11-11T02:52:34.354655Z","iopub.status.idle":"2023-11-11T02:52:34.361489Z","shell.execute_reply.started":"2023-11-11T02:52:34.354603Z","shell.execute_reply":"2023-11-11T02:52:34.360093Z"}}
# import requests
# import folium

# peta_brazil = requests.get('https://raw.githubusercontent.com/codeforgermany/click_that_hood/c920849a080627b584e139556ac009a509e73998/public/data/brazil-states.geojson').json()

# m = folium.Map(location=[-15, -45],
#                zoom_start=4)

# cp = folium.Choropleth(
#     geo_data=peta_brazil,
#     name="choropleth",
#     data=seller_prod_cnt_df,
#     columns=["seller_state", "product_id"],
#     key_on="properties.sigla",
#     fill_color="YlGn",
#     fill_opacity=0.7,
#     line_opacity=0.2,
#     legend_name="Count Order By state",
#     use_jenks=True,
#     highlight=True,
# ).add_to(m)

# seller_prod_cnt_df_indexed = seller_prod_cnt_df.set_index('seller_state')
# for s in cp.geojson.data['features']:
#     print(s['properties']['sigla'])
#     ccc = int(seller_prod_cnt_df_indexed.loc[s['properties']['sigla'],'product_id'])
#     s['properties']['seller_order'] = ccc
# # cp.geojson.data['features'][0]
# folium.GeoJsonTooltip(['name', 'seller_order']).add_to(cp.geojson)
# folium.LayerControl().add_to(m)
# m

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:34.363293Z","iopub.execute_input":"2023-11-11T02:52:34.363690Z","iopub.status.idle":"2023-11-11T02:52:35.065454Z","shell.execute_reply.started":"2023-11-11T02:52:34.363650Z","shell.execute_reply":"2023-11-11T02:52:35.064046Z"}}
# Create the countplot
# sns.set(style="darkgrid")
# plt.figure(figsize=(12, 6))
# ax = sns.countplot(
#     data=seller_df,
#     x="seller_state",
#     order=seller_df["seller_state"].value_counts().index,
# )
# ax.set(title="Banyak Penjual berdasarkan State")

# Add text labels to the bars
# for p in ax.patches:
#     ax.annotate(
#         f"{p.get_height()}",
#         (p.get_x() + p.get_width() / 2.0, p.get_height()),
#         ha="center",
#         va="center",
#         fontsize=12,
#         color="black",
#         xytext=(0, 10),
#         textcoords="offset points",
#     )

# plt.xticks(rotation=90)  # Rotate x-axis labels if needed
# plt.show()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:35.066809Z","iopub.execute_input":"2023-11-11T02:52:35.067212Z","iopub.status.idle":"2023-11-11T02:52:36.061076Z","shell.execute_reply.started":"2023-11-11T02:52:35.067178Z","shell.execute_reply":"2023-11-11T02:52:36.059714Z"}}
# Create the countplot
# sns.set(style="darkgrid")
# plt.figure(figsize=(12, 6))
# ax = sns.countplot(
#     data=cust_df,
#     x="customer_state",
#     order=cust_df["customer_state"].value_counts().index,
# )
# ax.set(title="Banyak Customer berdasarkan State")

# # Add text labels to the bars
# for p in ax.patches:
#     ax.annotate(
#         f"{p.get_height()}",
#         (p.get_x() + p.get_width() / 2.0, p.get_height()),
#         ha="center",
#         va="center",
#         fontsize=7,
#         color="black",
#         xytext=(0, 10),
#         textcoords="offset points",
#     )

# plt.xticks(rotation=90)  # Rotate x-axis labels if needed
# plt.show()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:36.063051Z","iopub.execute_input":"2023-11-11T02:52:36.063740Z","iopub.status.idle":"2023-11-11T02:52:37.548404Z","shell.execute_reply.started":"2023-11-11T02:52:36.063705Z","shell.execute_reply":"2023-11-11T02:52:37.547209Z"}}
# Create the countplot
# sns.set(style="darkgrid")
# plt.figure(figsize=(12, 15))
# ax = sns.countplot(
#     data=prod_df,
#     y="product_category_name",
#     order=prod_df["product_category_name"].value_counts().index,
# )
# ax.set(title="Banyak Produk berdasarkan Kategori")

# # Add text labels to the bars
# for p in ax.patches:
#     ax.annotate(
#         f"{p.get_width()}",
#         (p.get_width(), p.get_y() + p.get_height() / 2.0),
#         ha="center",
#         va="center",
#         fontsize=7,
#         color="black",
#         xytext=(10, 0),
#         textcoords="offset points",
#     )

# plt.xticks(rotation=0)
# plt.show()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:37.550144Z","iopub.execute_input":"2023-11-11T02:52:37.550615Z","iopub.status.idle":"2023-11-11T02:52:37.978927Z","shell.execute_reply.started":"2023-11-11T02:52:37.550575Z","shell.execute_reply":"2023-11-11T02:52:37.975644Z"}}
# sns.barplot(data=total_order_product[:10], x="order_item_id", y="product_category_name")

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:37.980370Z","iopub.execute_input":"2023-11-11T02:52:37.980740Z","iopub.status.idle":"2023-11-11T02:52:38.420887Z","shell.execute_reply.started":"2023-11-11T02:52:37.980710Z","shell.execute_reply":"2023-11-11T02:52:38.419371Z"}}
# Create the countplot
# sns.set(style="darkgrid")
# plt.figure(figsize=(12, 6))
# ax = sns.countplot(
#     data=order_df, x="order_status", order=order_df["order_status"].value_counts().index
# )
# ax.set(title="Banyak Customer berdasarkan State")

# # Add text labels to the bars
# for p in ax.patches:
#     ax.annotate(
#         f"{p.get_height()}",
#         (p.get_x() + p.get_width() / 2.0, p.get_height()),
#         ha="center",
#         va="center",
#         fontsize=7,
#         color="black",
#         xytext=(0, 10),
#         textcoords="offset points",
#     )

# plt.xticks(rotation=90)  # Rotate x-axis labels if needed
# plt.show()

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:38.422419Z","iopub.execute_input":"2023-11-11T02:52:38.422767Z","iopub.status.idle":"2023-11-11T02:52:39.364139Z","shell.execute_reply.started":"2023-11-11T02:52:38.422738Z","shell.execute_reply":"2023-11-11T02:52:39.362796Z"}}
order_review_df_ana = order_review_df.copy()
order_review_df_ana = order_review_df_ana[["review_id", "order_id", "review_score"]]
order_review_df_ana = order_review_df_ana.groupby(["order_id"]).agg(
    {"review_score": "mean"}
)
order_review_df_ana = order_review_df_ana.sort_values("review_score")
order_review_df_ana.columns = ["Mean_Review_Score"]

# sns.histplot(order_review_df_ana, x="Mean_Review_Score")

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:39.366008Z","iopub.execute_input":"2023-11-11T02:52:39.367577Z","iopub.status.idle":"2023-11-11T02:52:39.402907Z","shell.execute_reply.started":"2023-11-11T02:52:39.367532Z","shell.execute_reply":"2023-11-11T02:52:39.401754Z"}}
# order_pay_df[order_pay_df.duplicated(subset="order_id")]

# %% [markdown]
# ## Visualization & Explanatory Analysis

# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:39.404444Z","iopub.execute_input":"2023-11-11T02:52:39.404905Z","iopub.status.idle":"2023-11-11T02:52:42.193211Z","shell.execute_reply.started":"2023-11-11T02:52:39.404864Z","shell.execute_reply":"2023-11-11T02:52:42.192300Z"}}
peta_brazil = requests.get(
    "https://raw.githubusercontent.com/codeforgermany/click_that_hood/c920849a080627b584e139556ac009a509e73998/public/data/brazil-states.geojson"
).json()

m = folium.Map(location=[-15, -45], zoom_start=4)

cp = folium.Choropleth(
    geo_data=peta_brazil,
    name="choropleth",
    data=cust_ord_state_df,
    columns=["customer_state", "order_id"],
    key_on="properties.sigla",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Count Order By state",
    use_jenks=True,
    highlight=True,
).add_to(m)

cust_ord_state_df_indexed = cust_ord_state_df.set_index("customer_state")
for s in cp.geojson.data["features"]:
    ccc = int(cust_ord_state_df_indexed.loc[s["properties"]["sigla"], "order_id"])
    s["properties"]["n_order"] = ccc
# cp.geojson.data['features'][0]
folium.GeoJsonTooltip(["name", "n_order"]).add_to(cp.geojson)
folium.LayerControl().add_to(m)
st.title("Hasil Penjualan Barang Berdasarkan Customer")
mmap = folium_static(m, width=800)
# mmap
# %% [code] {"execution":{"iopub.status.busy":"2023-11-11T02:52:42.194630Z","iopub.execute_input":"2023-11-11T02:52:42.195204Z","iopub.status.idle":"2023-11-11T02:52:49.044904Z","shell.execute_reply.started":"2023-11-11T02:52:42.195170Z","shell.execute_reply":"2023-11-11T02:52:49.042092Z"}}

m_pin = folium.Map(location=[-15, -45], zoom_start=3)

for ind, spcd in seller_prod_cnt_df.iterrows():
    if (np.isnan(spcd.seller_lat) == False) and (np.isnan(spcd.seller_lat)) == False:
        folium.Marker(
            location=[spcd.seller_lat, spcd.seller_lng],
            tooltip=spcd.product_id,
            popup=spcd.seller_state,
            # icon=folium.Icon(icon="cloud"),
        ).add_to(m_pin)
st.title("Banyaknya Penjual di suatu daerah")
mpinku = folium_static(m_pin, width=800)
# mpinku

# st.markdown(
#     """
# # Kesimpulan

# * Lebih Banyak Penjualan berdasarkan Customer di Bagian Rio de Janiero, San Paulo, Minas Greais
# * Lebih Penjual di sekitar pantai di negara Brazil. dan biasanya ada di Rio de Janiero , San Paulo, dan Minas Greais
# * Berarti persebaran Penjual dan Pembeli tepat karena banyaknya wilayah penjual dan pembeli sama."""
# )
