import streamlit as st

st.write('# Analysis')

st.write('Now as we got the data, let\'s analyse it. ')

st.write('## Preparing the data')

code = '''
def get_dimensions(string):
    \'''
    This function gets artwork' dimensions.
    This function receives a string scraped from the website as input.
    This function returns a list with dimensions in centimeters.
    \'''
    dimensions = None
    try:
        if 'cm' in string:
            if re.findall('^[0-9]+[.][0-9]+ x [0-9]+[.][0-9]+ cm', string):
                dimensions = re.findall('[0-9]+[.][0-9]+ x [0-9]+[.][0-9]+ ', string)[0]
                digits = re.findall('[0-9]+[.][0-9]+', dimensions)
                dimensions = [float(digits[0]), float(digits[1])]
            elif re.findall('^[0-9]+ x [0-9]+ cm', string):
                dimensions = re.findall('[0-9]+ x [0-9]+ ', string)[0]
                digits = re.findall('[0-9]+', dimensions)
                dimensions = [float(digits[0]), float(digits[1])]
        if 'in' in string:
            if re.findall('^[0-9]+[.][0-9]+ x [0-9]+[.][0-9]+ in', string):
                dimensions = re.findall('[0-9]+[.][0-9]+ x [0-9]+[.][0-9]+ ', string)[0]
                digits = re.findall('[0-9]+[.][0-9]+', dimensions)
                dimensions = [float(digits[0])* 2.54, float(digits[1])* 2.54]
            elif re.findall('^[0-9]+ x [0-9]+ in', string):
                dimensions = re.findall('[0-9]+ x [0-9]+ ', string)[0]
                digits = re.findall('[0-9]+', dimensions)
                dimensions = [float(digits[0])* 2.54, float(digits[1])* 2.54]
    except TypeError:
        dimensions = None
    return dimensions
    
def auction_data_prepare(df):
    \'''
    This fuction prepares auction data for analysis: adds new variables, cleans everything up.
    \'''
    
    price_list = []
    area_list = []
    proportions_list = []
    auction_date_list = []

    for index, row in df.iterrows():
        ## price
        price_usd = row['price_usd']
        try: 
            price_usd = int(price_usd)
        except:
            price_usd = None
        ## dimensions
        size = row['size']
        try:
            dimensions = get_dimensions(size)
            area = dimensions[0] * dimensions[1]
            proportions = dimensions[0] / dimensions[1]
        except:
            area = None
            proportions = None
        ## date
        auction_date = row['auction_date']
        try:
            auction_date = pd.Timestamp(auction_date)
        except:
            auction_date = None
        

        ## putting it into an array
        price_list.append(price_usd)
        area_list.append(area)
        proportions_list.append(proportions)
        auction_date_list.append(auction_date)

    df['price_usd'] = pd.DataFrame(price_list, columns = ['price_usd'])['price_usd']
    df['auction_date'] = pd.DataFrame(auction_date_list, columns = ['auction_date'])['auction_date']
    df['area'] = pd.DataFrame(area_list, columns = ['area'])['area']
    df['proportions'] = pd.DataFrame(proportions_list, columns = ['proportions'])['proportions']
    df['year'] = df['auction_date'].dt.year
    df['month'] = df['auction_date'].dt.month
    df['name_len'] = df['name'].str.len()
    df['title_len'] = df['title'].str.len()
    
    df['day'] = '01'
    df['auction_month_year'] = pd.to_datetime(df[['year', 'month', 'day']])
    df = df.drop('day', axis = 1)
    
    return df
    
    data = pd.read_csv('auction_data.csv', dtype= str).drop(['Unnamed: 0', 'image_link'], axis = 1).drop_duplicates().reset_index().drop('index', axis = 1)
    data = auction_data_prepare(data)
    data.to_csv('data.csv')
    
'''
st.code(code, language='python')

with st.echo(code_location='below'):
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
    from PIL import Image
    import plotly.express as px
    import seaborn as sns



    st.write('## General')

    st.write('Let\'s take a look at the data: ')

    df = pd.read_csv('data.csv').drop('Unnamed: 0', axis = 1)
    st.dataframe(df)

    years = list(df.year.sort_values().unique())[:-1]
    names = list(df.name.value_counts().index)
    variables = ['price_usd', 'area', 'proportions', 'year', 'month', 'name_len', 'title_len']

    fig = px.imshow(df.corr(), title = 'Correlations')
    st.plotly_chart(fig)

    year = st.selectbox('I can show you correlations in a selected year', years, key = 'year1')
    fig = px.imshow(df[df['year'] == year].drop('year', axis = 1).corr(), title = f'Correlations in year {str(year)[:4]}')
    st.plotly_chart(fig)

    variable = st.selectbox('I can show you the individual distributions', variables, key = 'variable1')
    scale = st.selectbox('Choose a scale:', ['logarithmic', 'linear'], key = 'scale1')
    if scale == 'linear':
        fig, ax = plt.subplots()
        sns.histplot(data=df[variable], ax = ax, color = 'thistle')
        ax.set(xlabel=variable)
        ax.set_title(f'Distribution of {variable}')
        st.pyplot(fig)
    if scale == 'logarithmic':
        fig, ax = plt.subplots()
        sns.histplot(data=np.log(df[variable]), ax = ax, color = 'thistle')
        ax.set(xlabel=variable)
        ax.set_title(f'Distribution of {variable}, {scale} scale')
        st.pyplot(fig)

    variables = ['price_usd', 'area', 'proportions', 'name_len', 'title_len']

    df_for_plot_y_med = pd.DataFrame(df.groupby('year').median()).reset_index()
    df_for_plot_y_mean = pd.DataFrame(df.groupby('year').mean()).reset_index()
    df_for_plot_my_med = pd.DataFrame(df.groupby('auction_month_year').median()).reset_index()
    df_for_plot_my_mean = pd.DataFrame(df.groupby('auction_month_year').mean()).reset_index()

    st.write('Let\'s take a look at some timeseries charts.')

    variable = st.selectbox('Which variable would you like to plot?', variables, key = 'variable2')
    opt = st.selectbox('How would you like it plotted?', ['annual, median', 'annual, mean', 'monthly, median', 'monthly, mean'], key = 'option1')

    if opt == 'annual, median':
        fig = px.line(df_for_plot_y_med, x = 'year', y = variable, title = f'Plot of {variable}')
        st.plotly_chart(fig)
    if opt == 'annual, mean':
        fig = px.line(df_for_plot_y_mean, x = 'year', y = variable, title = f'Plot of {variable}')
        st.plotly_chart(fig)
    if opt == 'monthly, median':
        fig = px.line(df_for_plot_my_med, x = 'auction_month_year', y = variable, title = f'Plot of {variable}')
        st.plotly_chart(fig)
    if opt == 'monthly, mean':
        fig = px.line(df_for_plot_my_mean, x = 'auction_month_year', y = variable, title = f'Plot of {variable}')
        st.plotly_chart(fig)

    st.write('## Artists')

    st.write('Who are the top selling artists in our sample?')

    data_artists = pd.DataFrame(df.groupby('name').sum().sort_values(by = 'price_usd', ascending = False)[:25]['price_usd']).reset_index()
    fig = px.bar(data_artists, x='name', y='price_usd', title='Top 25 artists by volume of artwork sold in the sample',
                 labels= {'name': 'Artist\'s name', 'price_usd': 'Volume of art sold, USD'})
    st.plotly_chart(fig)

    st.write('Whose artwork dimensions are the largest?')

    data_artists = pd.DataFrame(df.groupby('name').median().sort_values(by = 'area', ascending = False)[:25]['area']).reset_index()
    fig = px.bar(data_artists, x='name', y='area', title='Top 25 artists by median artwork size in the sample',
                 labels= {'name': 'Artist\'s name', 'area': 'Artwork\'s dimensions in cm^2'})
    st.plotly_chart(fig)

    st.write('Hmmm. David Adjaye has a median 1M sq cm artwork size. Who could that guy be? Oh, wait...')
    st.image(Image.open('image_2.png'), caption='Сколково не забыто')

    artists = list(pd.DataFrame(df.groupby('name').sum().sort_values(by = 'price_usd', ascending = False)).index)
    artist = st.selectbox('Which artist would you like to look at?', artists, key = 'artist')
    data_artists = df[df['name'] == artist].groupby('year').median().reset_index()
    fig = px.line(data_artists, x='year', y='price_usd', title=f'Median price of {artist}\'s art sold',
                 labels= {'name': 'Date', 'price_usd': 'Price, USD'})
    st.plotly_chart(fig)

    try:
        piece = df[df['name'] == artist].sort_values('price_usd', ascending=False).iloc[0].title
        price = df[df['name'] == artist].sort_values('price_usd', ascending=False).iloc[0].price_usd
        st.write(f'Fun info: most expensive piece of art produced by {artist} is {piece} that sold for {int(price)} dollars.')
    except:
        pass



    if st.button('клик ми'):
        image = Image.open('image.jpg')
        st.image(image, caption='Есть ли смысл?')


    st.write('## Source code')
