from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context

from utils.duckaccess import DuckSession

from pandas import DataFrame

import numpy as np
from collections import Counter
from pandas import DataFrame, merge

class ScaleTo10000(AbstractHandler):

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ((size * 10000) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scaled_size = duck.sql("SELECT SUM(scaled_size) from scaled""").fetchone()[0]
            print(f"Scaled Size: {scaled_size}")

            scale_aux = list(scaled["scaled_size"])
            for i, row in scaled.iterrows():
                if sum(scale_aux) == 10000:
                    break

                if row["size"] > 0 and scale_aux[i] == 0:
                    scale_aux[i] = 1

            scale_aux_size = sum(scale_aux)
            print(f"Scaled Size Fixed: {scale_aux_size}")
            assert scale_aux_size == 10000

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleTo10000_2(AbstractHandler):

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ((size * 10000) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scaled_size = duck.sql("SELECT SUM(scaled_size) from scaled""").fetchone()[0]
            print(f"Scaled Size: {scaled_size}")

            scale_aux = list(scaled["scaled_size"])
            for i, row in scaled.iterrows():
                if sum(scale_aux) == 10000:
                    break
                
                if row["size"] > 1 and scale_aux[i] < 2:
                    scale_aux[i] = 2

                if row["size"] > 0 and scale_aux[i] == 0:
                    scale_aux[i] = 1

            scale_aux_size = sum(scale_aux)
            print(f"Scaled Size Fixed: {scale_aux_size}")

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        assert scale_aux_size == 10000

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleTo10000_3(AbstractHandler):

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ((size * 10000) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scaled_size = duck.sql("SELECT SUM(scaled_size) from scaled""").fetchone()[0]
            print(f"Scaled Size: {scaled_size}")

            scale_aux = list(scaled["scaled_size"])
            for i, row in scaled.iterrows():
                if sum(scale_aux) == 10000:
                    break
                
                if row["size"] > 1 and scale_aux[i] < 2:
                    scale_aux[i] = 2

                if row["size"] > 0 and scale_aux[i] == 0:
                    scale_aux[i] = 1

            scale_aux_size = sum(scale_aux)
            print(f"Scaled Size Fixed: {scale_aux_size}")

            top_10_greatest = sorted(scale_aux, reverse=True)[:10]
            top_10_greatest_index = list(map(lambda x: top_10_greatest.index(x), top_10_greatest))

            increments = int((10000 - scale_aux_size) / len(top_10_greatest))

            for i in top_10_greatest_index:
                scale_aux[i] = scale_aux[i] + increments

            print(f"Double Scaled Size Fixed: {sum(scale_aux)}")

            scale_aux[top_10_greatest_index[0]] = scale_aux[top_10_greatest_index[0]] + 2

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        assert sum(scale_aux) == 10000

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleTo(AbstractHandler):

    def __init__(self, scale_size=10000) -> None:
        super().__init__()
        self.scale_size = scale_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            print(self.scale_size)

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , FLOOR((size * {self.scale_size}) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scale_aux = list(scaled["scaled_size"])

            print(f"Scaled Size: {sum(scale_aux)}")

            tags = list(original_sizes["home_ageb"])
        
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        assert sum(scale_aux) >= self.scale_size

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleBySample(AbstractHandler):

    def __init__(self, sample_size=10000) -> None:
        super().__init__()
        self.__sample_size = sample_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            """).df()
            #plt.bar(original_sizes["home_ageb"], original_sizes["size"])
            #original_sizes.hist("size")
            #plt.savefig("./images/original_dist.png")

            sizes_with_probs = duck.sql("""
            SELECT *
                , (size / total)::FLOAT AS prob
            FROM (
                SELECT *
                    , (SUM(size) OVER())::DECIMAL(18,3) AS total
                FROM original_sizes
            )
            """).df()
            #assert int(sum(list(sizes_with_probs["prob"]))) == 1

            agebs_list = list(sizes_with_probs["home_ageb"])
            probs_list = list(sizes_with_probs["prob"])

            np.random.seed(3696)
            sample = np.random.choice(agebs_list, size=self.__sample_size, p=probs_list, replace=True,)
            assert len(sample) == self.__sample_size


            ageb_counter = Counter(list(sample))
            scaled_df = DataFrame(list(ageb_counter.items()))
            print(scaled_df.dtypes)
            scaled_df = scaled_df.rename(columns={scaled_df.columns[0] : "home_ageb"
                                        , scaled_df.columns[1]: "scaled_size"})
            
            
            print(f"Sum scaled_size: {sum(list(scaled_df['scaled_size']))}")

            final_scale = merge(original_sizes, scaled_df, left_on="home_ageb", right_on="home_ageb", how="left")
            final_scale["scaled_size"] = final_scale["scaled_size"].apply(lambda x: 0 if np.isnan(x) else int(x))
            final_scale = final_scale[["home_ageb", "scaled_size"]].sort_values(by="home_ageb")
            #plt.bar(final_scale["home_ageb"], final_scale["scaled_size"])
            #plt.clf()
            #final_scale.hist("scaled_size")
            #plt.savefig("./images/scaled_dist.png")

            assert original_sizes.shape == final_scale.shape

            final_scale = list(final_scale["scaled_size"])
            assert sum(list(final_scale)) == self.__sample_size

            tags = list(original_sizes["home_ageb"])

            context.payload = (final_scale, list(zip(tags, final_scale)))

        return context
        

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))