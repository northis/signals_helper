//signals_verifier.mq5
//Copyright 2021, n0rth

#include <Trade\Trade.mqh>
#include <Trade\PositionInfo.mqh>
#include <Trade\AccountInfo.mqh>
#include <Trade\SymbolInfo.mqh>


input ulong    EA_Magic=3300187;   // Magic Number
input double   Lot=0.1;          // Volume for orders
input int      Min_Bars = 20;    // Minimal amount bars for trading
input int    Deviation=100;       //max deviation in points

CTrade mytrade;
CPositionInfo myposition;
CAccountInfo myaccount;
CSymbolInfo mysymbol;

double plsDI[],minDI[];
double rocMinor[], rocMain[];
double priceCloseMinor, priceCloseMain;
int STP,TKP;
double TPC;

//checks that the advisor can trade
bool checkTrading()
{
   bool can_trade=false;
   if(myaccount.TradeAllowed() && myaccount.TradeExpert() && mysymbol.IsSynchronized()) {
      int mbars=Bars(_Symbol,_Period);
      if(mbars>Min_Bars) {
         can_trade=true;
      }
   }
   return(can_trade);
}

// confirms that we have margin enough to trade
bool ConfirmMargin(ENUM_ORDER_TYPE otype,double price)
{
   bool confirm=false;
   double lot_price=myaccount.MarginCheck(_Symbol,otype,Lot,price);
   double act_f_mag=myaccount.FreeMargin();

   if(MathFloor(act_f_mag*TPC)>MathFloor(lot_price)) {
      // we have free margin enough
      confirm=true;
   }
   return(confirm);
}

//cloese the position if needed
bool ClosePosition(string ptype,double clp)
{
   bool marker=false;

   if(myposition.Select(_Symbol)==true && myposition.Magic()==EA_Magic && myposition.Symbol()==_Symbol) {
      if(mytrade.PositionClose(_Symbol)) {
         Alert("The position has beed closed");
         marker=true;
      } else {
         Alert("Cannot close the position, error: ",mytrade.ResultRetcodeDescription());
      }
   }
   return(marker);
}

//Expert initialization function
int OnInit()
{
   mysymbol.Name(_Symbol);
   mytrade.SetExpertMagicNumber(EA_Magic);
   mytrade.SetDeviationInPoints(Deviation);
   return(INIT_SUCCEEDED);
}

//Expert deinitialization function
void OnDeinit(const int reason){
}

//Expert tick function
void OnTick()
{
   if(checkTrading()==false) {
      Alert("The advisor cannot trade - check the requirements");
      return;
   }
   MqlRates MainMRate[];

   ArraySetAsSeries(MainMRate,true);

   if(!mysymbol.RefreshRates()) {
      Alert("Cannot refresh rates: ",GetLastError());
      return;
   }

   if(CopyRates(_Symbol,_Period,0,1,MainMRate)<0) {
      Alert("Cannot copy historical data: ",GetLastError());
      return;
   }

   //--- check the conditions on new bar arrival only
   static datetime MainPrevTime;

   datetime MainBarTime[1];

   MainBarTime[0]=MainMRate[0].time;

   if(MainPrevTime==MainBarTime[0]) {
      // no new bar
      return;
   }

   priceCloseMain=MainMRate[0].close;

   //  if(ClosePosition("BUY",priceCloseMinor)==true) {
   //             return;
   //          } else {
   //             Print("Cannot close long");
   //          }
   //          if(ClosePosition("SELL",priceCloseMinor)==true) {
   //             return;
   //          } else {
   //             Print("Cannot close short");
   //          }
   
   // double mprice=NormalizeDouble(mysymbol.Ask(),_Digits);                //--- последняя цена ask
   // double userSL = NormalizeDouble(mysymbol.Ask() - STP*_Point,_Digits);  
   
   // if(ConfirmMargin(ORDER_TYPE_BUY,mprice)==false) {
   //    Alert("Insufficient funds");
   //    return;
   // }

   // if(mytrade.Buy(Lot,_Symbol,mprice,userSL,0)) {
   //    Alert("Buy order has been placed with ticket #",mytrade.ResultDeal());
   // } else {
   //    Alert("Cannot place buy ticket with volume :",mytrade.RequestVolume(),
   //          ", sl:", mytrade.RequestSL(),", tp:",mytrade.RequestTP(), ", price:",
   //          mytrade.RequestPrice(), " error:",mytrade.ResultRetcodeDescription());
   //    return;
   // }
      // double sprice=NormalizeDouble(mysymbol.Bid(),_Digits);   
   // double userSL = NormalizeDouble(mysymbol.Bid()+STP*_Point,_Digits);      
   
   // if(ConfirmMargin(ORDER_TYPE_SELL,sprice)==false) {
   //    Alert("Insufficient funds");
   //    return;
   // }

   // if(mytrade.Sell(Lot,_Symbol,sprice,userSL,0)) {
   //    Alert("Sell order has been placed with ticket #",mytrade.ResultDeal());
   // } else {
   //    Alert("Cannot place sell ticket with volume :",mytrade.RequestVolume(), ", price:",
   //          mytrade.RequestSL(),", tp:",mytrade.RequestTP(), ", цена:", mytrade.RequestPrice(),
   //          " error:",mytrade.ResultRetcodeDescription());
   //    return;
   // }

}
