select sum(diff)
from (select CASE IsBuy 
           WHEN 0 
               THEN (PriceActual - ClosePrice)
           ELSE 
				(ClosePrice - PriceActual)
       END diff
    from 'Order'
    where ErrorState is 'NULL';
	   