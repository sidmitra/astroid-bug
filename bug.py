from pandas import DataFrame, pivot_table, read_sql



def mymethod(integration_id: int) -> DataFrame:
    query1 = f"""_____"""
    query2 = f"""_____"""
    query3 = f"""_____"""

    df_short_term_outstanding = read_sql(query1, connection, index_col="account")
    df_long_term_outstanding = read_sql(query2, connection, index_col="account")
    df_prior_unrecognised = read_sql(query3, connection, index_col="account")

    df_short_term_outstanding = pivot_table(
        df_short_term_outstanding, index="account", columns="month", aggfunc="sum"
    )
    df = df_short_term_outstanding.join(
        df_prior_unrecognised, on="account", how="left"
    ).join(df_long_term_outstanding, on="account", how="left")
    df = df.fillna(0)
    df.insert(
        0,
        "prepayment account",
        [get_account_name(item.split("--")[0]) for item in df.index],
    )
    df.insert(
        1,
        "expense account",
        [get_account_name(item.split("--")[1]) for item in df.index],
    )
    df["total"] = df.sum(axis=1)
    df.loc["Total"] = df.sum(numeric_only=True)
    df.at["Total", "prepayment account"] = "Total"

    rearranged_columns = (
        ["prepayment account", "expense account", "prior_unrecognised"]
        + [
            item
            for item in df.columns
            if item
            not in [
                "prepayment account",
                "expense account",
                "prior_unrecognised",
                "future",
                "total",
            ]
        ]
        + ["future", "total"]
    )
    df = df[rearranged_columns]

    new_columns = []
    for item in df.columns:
        if len(item) == 2:
            item = item[1].strftime("%b %Y")
        item = item.replace("_", " ").title()
        new_columns.append(item)
    df.columns = new_columns

    return df
