import pandas as pd
from Levenshtein import ratio
from pandarallel import pandarallel

pd.options.mode.chained_assignment = None
# apply легко параллелится поэтому в данном кейсе использовать эту фичу выгодно
pandarallel.initialize(nb_workers=12)

# функция для подстчета процента отклонения от заданной подстроки
# для начала для каждого названия посчитаем процент с каждым названием из основного каталога, затем выберем лучший %
def lev_ratio(x):
    ratio_max = 0
    ratio_max_ind = 0

    for index, row in main.iterrows():
        curr_ratio = ratio(x["НоменклатураНаименованиеПолное"], row["НоменклатураНаименованиеПолное"])
        ratio_max = max(ratio_max, curr_ratio)
        if ratio_max == curr_ratio:
            ratio_max_ind = row["Штрихкод"]

    return {ratio_max_ind: ratio_max}
# функция для подсчета тех совпдаений где совпало 95%+ символов
def lev_perc(x):
    global pr95
    global pr_all

    for lev in x.lev_ratio:
        if x.lev_ratio[lev] >= 0.95:
            pr95 += 1
        else:
            pr_all += 1

# перевод в стр для сравнения артикулов
def to_str(x):
    try:
        return str(float(x["НоменклатураАртикул"]))
    except:
        return "0"

# загрузка данных
main = pd.read_excel("csv/Основной.xls")
dealer1 = pd.read_excel("csv/Дилер.xlsx")
dealer2 = pd.read_csv("csv/Дилер 2.csv", encoding="cp1251", on_bad_lines='skip', sep=";", header=None)

# приводим дааные от дилера1 к общему виду чтобы проще было сравнивать
dealer1 = dealer1.iloc[1: , :]
new_header = dealer1.iloc[0]
dealer1 = dealer1.iloc[1: , :]
dealer1.columns = new_header
dealer1 = dealer1[dealer1.columns.dropna()]

# приводим дааные от дилера2 к общему виду чтобы проще было сравнивать
dealer2.rename(columns ={8:"НоменклатураНаименованиеПолное", 7:"НоменклатураАртикул", 12:"Штрихкод"}, inplace=True)

dealers = {"Дилер 2": dealer2, "Дилер 1": dealer1}

for row_dealer in dealers:
    dealer = dealers[row_dealer]
    # вначале посчитаем по Артикулам, для этого нужно дропнуть строчки где нет артикула
    dealer_df = dealer[dealer["НоменклатураАртикул"].notna()]
    main_df = main[main["НоменклатураАртикул"].notna()]

    main_df['НоменклатураАртикул'] = main_df.apply(lambda x: to_str(x), axis=1)
    dealer_df['НоменклатураАртикул'] = dealer_df.apply(lambda x: to_str(x), axis=1)

    main_df = main_df[main_df["НоменклатураАртикул"] != "0"]
    dealer_df = dealer_df[dealer_df["НоменклатураАртикул"] != "0"]

    # смержим данные по артилкулу
    df_indx = main_df.merge(dealer_df[dealer_df["НоменклатураАртикул"].notna()], on="НоменклатураАртикул", how="inner")

    # смержим данные по Полному Названию
    df_name = main.merge(dealer1[dealer1["НоменклатураНаименованиеПолное"].notna()],
                         on="НоменклатураНаименованиеПолное", how="inner")

    # т.к. в ТЗ требуется посчитать их вместе то соеденим
    df_ind_name = df_indx.append(df_name).drop_duplicates(subset=["Штрихкод_y"])

    # поиск расстояния Левенштейна
    df_lev = dealer1.copy()

    # уберем те строчки которые мы уже смержили в пред. пункте
    for index, row in df_ind_name.iterrows():
        df_lev = df_lev[df_lev.НоменклатураНаименованиеПолное != row["НоменклатураНаименованиеПолное_y"]]
        df_lev = df_lev[df_lev.НоменклатураАртикул != row["НоменклатураАртикул"]]

    # посчитаем % совпадения
    df_lev['lev_ratio'] = df_lev.parallel_apply(lambda x: lev_ratio(x), axis=1)

    pr95 = 0
    pr_all = 0

    # соберем статистику по %
    df_lev.apply(lambda x: lev_perc(x), axis=1)

    #  вывод данных, записей может быть больше чем изначально в дф Дилера т.к. некоторые строчки в основном дф совпадают
    print(row_dealer)
    print("Записей всего %s" % str(len(df_lev) + len(df_ind_name)))
    print("Меппинг по «Артикулу» и «Полному наименованию» %s" % len(df_ind_name))
    print("Меппинг по алгортиму с вероятностью более 95%% %s" % pr95)
    print("Меппинг по алгортиму с вероятностью менее 95%% %s" % pr_all)

