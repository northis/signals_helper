select Date as "open", CloseDate as "close", Symbol as symbol, CASE IsBuy 
           WHEN 0 
               THEN 'sell'
           ELSE 
				'buy'
       END "action", round(PriceActual,1) as "open price", o.StopLoss as sl, o.TakeProfit as tp, c.Name, ClosePrice as "close price",
	 round(CASE IsBuy WHEN 0 THEN o.PriceActual - o.ClosePrice  ELSE 				o.ClosePrice - o.PriceActual       END,2) diff,
	 o.PriceSignal as ps,
	round(CASE IsBuy WHEN 0 THEN o.StopLoss - o.PriceSignal  ELSE 				o.PriceSignal - o.StopLoss       END,2) sl_act
FROM 'Order' o join Channel c on (c.Id = o.IdChannel)
where CloseDate is not null and o.ErrorState is null
    and IdChannel in (1317030692) 
