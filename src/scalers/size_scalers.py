from typing import Any
from core.core_abstract import AbstractHandler
from core.context import Context

from utils.duckaccess import DuckSession

from pandas import DataFrame

import numpy as np
from collections import Counter
from pandas import DataFrame, merge

import matplotlib.pyplot as plt

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
    
class ScaleTo_1(AbstractHandler):

    def __init__(self, scale_size=10000) -> None:
        super().__init__()
        self.scale_size = scale_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]
            plt.clf()
            plt.title(f"Original Size in {context.year}-{context.month}-{context.day}: {N:,}")
            #original_sizes.hist("size")
            plt.hist(original_sizes["size"])
            plt.savefig("./images/original_dist.png")

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ((size * {self.scale_size}) / {N})::INTEGER AS scaled_size
                --, CEIL(CEIL(size * {self.scale_size}) / {N})::INTEGER AS scaled_size
                --, ROUND(CEIL(size * {self.scale_size}) / {N})::INTEGER AS scaled_size_4
            FROM original_sizes
            """).df()
            scale_aux = list(scaled["scaled_size"])
            plt.clf()
            plt.title(f"Scaled Size (A) in {context.year}-{context.month}-{context.day}: {sum(scale_aux):,}")
            #scaled.hist("scaled_size")
            plt.hist(scaled["scaled_size"])
            plt.savefig("./images/scaled_dist_A.png")


            print(f"Scaled Size: {sum(scale_aux)}")

            tags = list(original_sizes["home_ageb"])

            # context.payload = (original_sizes, DataFrame(list(zip(tags, scale_aux))))
            #context.payload = scaled
        
        # THIS IS THE FINAL PAYLOAD
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        #assert sum(scale_aux) >= self.scale_size

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleTo_2(AbstractHandler):

    def __init__(self, scale_size=10000) -> None:
        super().__init__()
        self.scale_size = scale_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]
            plt.clf()
            plt.title(f"Original Size in {context.year}-{context.month}-{context.day}: {N:,}")
            #original_sizes.hist("size")
            plt.hist(original_sizes["size"])
            plt.savefig("./images/original_dist.png")

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , CEIL(CEIL(size * {self.scale_size}) / {N})::INTEGER AS scaled_size
                --, ROUND(CEIL(size * {self.scale_size}) / {N})::INTEGER AS scaled_size_4
            FROM original_sizes
            """).df()
            scale_aux = list(scaled["scaled_size"])
            plt.clf()
            plt.title(f"Scaled Size (B) in {context.year}-{context.month}-{context.day}: {sum(scale_aux):,}")
            #scaled.hist("scaled_size")
            plt.hist(scaled["scaled_size"])
            plt.savefig("./images/scaled_dist_B.png")


            print(f"Scaled Size: {sum(scale_aux)}")

            tags = list(original_sizes["home_ageb"])

            # context.payload = (original_sizes, DataFrame(list(zip(tags, scale_aux))))
            #context.payload = scaled
        
        # THIS IS THE FINAL PAYLOAD
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        #assert sum(scale_aux) >= self.scale_size

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))

class ScaleTo_3(AbstractHandler):

    def __init__(self, scale_size=10000) -> None:
        super().__init__()
        self.scale_size = scale_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            original_sizes = duck.sql(f"""
            SELECT *
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]
            plt.clf()
            plt.title(f"Original Size in {context.year}-{context.month}-{context.day}: {N:,}")
            #original_sizes.hist("size")
            plt.hist(original_sizes["size"])
            plt.savefig("./images/original_dist.png")

            print(f"Original Size: {N}")

            scaled = duck.sql(f"""
            SELECT *
                , ROUND(CEIL(size * {self.scale_size}) / {N})::INTEGER AS scaled_size
            FROM original_sizes
            """).df()
            scale_aux = list(scaled["scaled_size"])
            plt.clf()
            plt.title(f"Scaled Size (C) in {context.year}-{context.month}-{context.day}: {sum(scale_aux):,}")
            #scaled.hist("scaled_size")
            plt.hist(scaled["scaled_size"])
            plt.savefig("./images/scaled_dist_C.png")


            print(f"Scaled Size: {sum(scale_aux)}")

            tags = list(original_sizes["home_ageb"])

            # context.payload = (original_sizes, DataFrame(list(zip(tags, scale_aux))))
            #context.payload = scaled
        
        # THIS IS THE FINAL PAYLOAD
        context.payload = (scale_aux, list(zip(tags, scale_aux)))
            
        #assert sum(scale_aux) >= self.scale_size

        return context

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))
    
class ScaleBySample(AbstractHandler):

    def __init__(self, sample_size=10000) -> None:
        super().__init__()
        self.__sample_size = sample_size

    def scale(self, context: Context):

        with DuckSession() as duck:

            np.set_printoptions(precision=10)

            # 1. Get the sizes table. Ensure an ascending ordering
            original_sizes = duck.sql(f"""
            SELECT *
                , 1 AS single_representant
            FROM read_parquet('{context.base_dir}/sizes.parquet')
            ORDER BY 1 ASC
            """).df()
            N = duck.sql("SELECT SUM(size) from original_sizes""").fetchone()[0]
            plt.clf()
            plt.title(f"Original Size in {context.year}-{context.month}-{context.day}: {N:,}")
            plt.hist(original_sizes["size"])
            plt.savefig("./images/original_dist.png")

            # 2. Compute the probabilities asociated with each Ageb
            original_sizes_arr = np.array(list(original_sizes["size"]))
            total = np.sum(original_sizes_arr)
            probs = np.array(list(original_sizes["size"])) / total
            print(type(probs))
            probs /= probs.sum()
            print(total)
            print(sum(probs))

            ## Probs must sum 1
            assert np.sum(probs) == 1.0

            # 3. Compute the difference between the sum of the single representants
            ## and the expected size
            sum_of_single_rep = sum(list(original_sizes["single_representant"]))
            difference = self.__sample_size - sum_of_single_rep
            print(sum_of_single_rep)
            print(difference)

            # 4. Obtain a sample of size equal to the difference
            agebs_list = list(original_sizes["home_ageb"])
            np.random.seed(3696)
            sample = np.random.choice(agebs_list, size=difference, p=probs, replace=True)

            assert len(sample) == difference

            # 5. Count the ocurrences in the difference
            ageb_counter = Counter(list(sample))
            scaled_df = DataFrame(list(ageb_counter.items()))
            print(scaled_df.dtypes)
            scaled_df = scaled_df.rename(columns={scaled_df.columns[0] : "home_ageb"
                                        , scaled_df.columns[1]: "aux_size"})
            
            print(f"Sum scaled_size: {sum(list(scaled_df['aux_size']))}")

            # 6. Join the sampled size with the size due to single representants
            final_scale = merge(original_sizes, scaled_df, left_on="home_ageb", right_on="home_ageb", how="left")
            final_scale["aux_size"] = final_scale["aux_size"].apply(lambda x: 0 if np.isnan(x) else int(x))
            print(final_scale)
            final_scale["scaled_size"] = final_scale[["single_representant", "aux_size"]].apply(lambda x: x["single_representant"] \
                                                                                              if np.isnan(x["aux_size"]) \
                                                                                                else x["single_representant"] + x["aux_size"], axis=1)
            original_sizes = original_sizes[["home_ageb", "size"]]
            final_scale = final_scale[["home_ageb", "scaled_size"]].sort_values(by="home_ageb")

            scale_aux = list(final_scale["scaled_size"])

            plt.clf()
            plt.title(f"Scaled Size (D) in {context.year}-{context.month}-{context.day}: {sum(scale_aux):,}")
            plt.hist(final_scale["scaled_size"])
            plt.savefig("./images/scaled_dist_D.png")

            assert original_sizes.shape == final_scale.shape

            final_scale = list(final_scale["scaled_size"])
            assert sum(list(final_scale)) == self.__sample_size

            tags = list(original_sizes["home_ageb"])

            context.payload = (final_scale, list(zip(tags, final_scale)))

        return context
        

    def handle(self, request: Any) -> Any:
        return super().handle(self.scale(request))