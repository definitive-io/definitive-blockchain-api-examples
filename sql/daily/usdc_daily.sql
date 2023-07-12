select day, total_supply as circulating_supply 
from `prj-p-bi-cust-b30f93b5-00`.`ethereum_v1_0_x`.`tokens_daily`
where token_address = lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')  -- USD Coin (USDC) on Ethereum
order by day 
limit 1000
