class Transform():  
    def __init__(self) -> None:
        self.process = 'Transform Process'


    def enunciado1(self, df_customer, df_orders, df_order_items):
        df_enunc1_m = df_customer[["customer_id","customer_fname", "customer_lname", "customer_email"]]\
                            .merge(df_orders[["order_id", "order_customer_id", "order_status"]], left_on="customer_id", right_on="order_customer_id" , how="inner")\
                            .merge(df_order_items[["order_item_order_id", "order_item_quantity", "order_item_subtotal"]], left_on="order_id", right_on="order_item_order_id",how="inner")
                    
        df_enunc1_q = df_enunc1_m[["customer_id", "order_status", "order_item_quantity", "order_item_subtotal"]]\
                        .query("order_status != 'CANCELED'")\
                        .groupby(["customer_id"])\
                        .sum()\
                        .sort_values(["order_item_subtotal"], ascending=False)

        df_enunc1_q["customer_id"] = df_enunc1_q.index
        df_enunc_im = df_customer[["customer_id","customer_fname", "customer_lname", "customer_email"]].merge(df_enunc1_q.reset_index(drop=True), on = "customer_id", how = "inner")
        
        df_enunc_im.rename(columns={'order_item_quantity':'quantity_item_total', 'order_item_subtotal':'total'}, inplace=True)
        df_enunc1 = df_enunc_im.sort_values(by=["total"], ascending = 0).head(20)
        
        return df_enunc1
        

    def enunciado2(self, df_order_items, df_products, df_categories):
        df_enunc2_m = df_order_items[["order_item_product_id", "order_item_quantity", "order_item_subtotal"]]\
                        .merge(df_products[["product_id", "product_category_id"]], left_on = "order_item_product_id", right_on = "product_id", how = "inner")\
                        .merge(df_categories[["category_id", "category_name"]], left_on = "product_category_id", right_on = "category_id", how = "inner")
        
        df_enunc2_q = df_enunc2_m[["category_name", "order_item_quantity", "order_item_subtotal"]]\
                        .groupby(["category_name"])\
                        .sum()
        
        df_enunc2_q["category_name"] = df_enunc2_q.index
        df_enunc2_i = df_enunc2_q[["category_name", "order_item_quantity", "order_item_subtotal"]].reset_index(drop = True)

        df_enunc2_i["order_item_subtotal"] = df_enunc2_i['order_item_subtotal'].astype('int')
        df_enunc2_i.rename(columns={'order_item_quantity':'item_quantity', 'order_item_subtotal':'total'}, inplace = True)
        df_enunc2 = df_enunc2_i

        return df_enunc2


    def enunciado3(self, df_customer, df_orders, df_order_items, df_products, df_categories):   
           df_enunc3 = df_customer[["customer_id", "customer_city"]]\
                                        .merge(df_orders[["order_customer_id", "order_id"]], left_on = "customer_id", right_on = "order_customer_id", how = "inner")\
                                        .merge(df_order_items[["order_item_order_id", "order_item_product_id"]], left_on = "order_id", right_on = "order_item_order_id", how = "inner")\
                                        .merge(df_products[["product_id", "product_category_id"]], left_on = "order_item_product_id", right_on = "product_id")\
                                        .merge(df_categories[["category_id", "category_name"]], left_on = "product_category_id", right_on = "category_id", how = "inner")
           
           df_enunc3_g = df_enunc3.groupby(['customer_city', 'category_name']).size().reset_index(name='count')
           df_enunc3_g['rank'] = (df_enunc3_g.groupby(["customer_city"])['count'].rank(method='dense', ascending=0).astype(int))
           
           df_enunc3_g.rename(columns={"count":"quantity"}, inplace = True)
           df_enunc3 = df_enunc3_g[["customer_city", "category_name", "quantity", "rank"]].query("rank == 1")
           
           return df_enunc3
    

    def enunciado4(self, df_customer, df_orders, df_order_items, df_products, df_categories):
         df_enunc4_m = df_customer[["customer_id", "customer_city"]]\
                            .merge(df_orders[["order_customer_id", "order_id"]], left_on = "customer_id", right_on = "order_customer_id", how = "inner")\
                            .merge(df_order_items[["order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal"]], left_on = "order_id", right_on = "order_item_order_id", how = "inner")\
                            .merge(df_products[["product_id", "product_category_id", "product_name"]], left_on = "order_item_product_id", right_on = "product_id")\
                            .merge(df_categories[["category_id", "category_name"]], left_on = "product_category_id", right_on = "category_id", how = "inner")
         
         df_enunc4_s = df_enunc4_m[["customer_city", "product_name", "order_item_quantity", "order_item_subtotal"]]\
                            .groupby(["customer_city", "product_name"])\
                            .sum()
         
         df_enunc4_s['rank'] = (df_enunc4_s.groupby(["customer_city"])['order_item_quantity'].rank(method='dense', ascending=0).astype(int))
         df_enunc4_s.rename(columns={"order_item_quantity":"quantity", "order_item_subtotal":"total"}, inplace = True)

         df_top_5_by_ciudad = (df_enunc4_s.query("rank < 6").sort_values(['quantity'], ascending=0))
         df_enunc4 = df_top_5_by_ciudad.reset_index()

         df_enunc4.groupby('customer_city').head(5).reset_index(drop=True)

         return df_enunc4
    
    
    def enunciado5(self, df_orders, df_order_items, df_products, df_categories, df_departments):        
         df_enunc5_m  = df_orders.merge(df_order_items, left_on='order_id', right_on='order_item_order_id')\
                                .merge(df_products, left_on='order_item_product_id', right_on='product_id')\
                                .merge(df_categories, left_on='product_category_id', right_on='category_id')\
                                .merge(df_departments, left_on='category_department_id', right_on='department_id')
         
         df_enunc5_m ["year"]= df_enunc5_m['order_date'].str[0:4]
         df_enunc5_m ['year'] = df_enunc5_m['year'].astype('int32')

         df_enunc5_s = df_enunc5_m[['year','department_name','order_item_subtotal']].groupby(['year','department_name']).sum(['order_item_subtotal'])
         df_enunc5 = df_enunc5_s.reset_index()

         return df_enunc5




    

    