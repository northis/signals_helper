
class SignalProps {
    [int]$Id;
    [bool]$IsBuy;
    [bool]$IsSlTpDelayed;
    [decimal]$Price;
    [Collections.Generic.List[decimal]]$TakeProfits;
    [decimal]$StopLoss;
    [string]$DatetimeUtc;
    [MessageProps]$MoveSlToEntry;
    [Collections.Generic.List[MessageProps]]$TpHit;
    [Collections.Generic.List[MessageProps]]$MoveSlToProfit;
    [MessageProps]$SlHit;
    [MessageProps]$Exit;
}

class MessageProps {
    [int]$Id;
    [int]$IdRef;
    [string]$DatetimeUtc;
    [string]$Text;
    [decimal]$Price;
}

$mainJson = Get-Content ".\ElliottWaveVIP.json" | ConvertFrom-Json;
$signalsResult = @{};
$serviceMessageLengthMax = 200;
$symbolLowerRegex = "(gold)|(xau)|(xauusd)";
$symbolName = "xauusd";
$channelName = "ElliottWaveVIP";

$lastSignal = [SignalProps]@{};
$style = [Globalization.NumberStyles]::Float;
$culture = [cultureinfo]::GetCultureInfo('en-US');

foreach ($item in $mainJson.messages) {
    $text = ($item.text | ConvertTo-Json -depth 100).Replace("[", "").Replace("]", "").Replace("{", "").Replace("}", "").Replace("`"", "").ToLower();

    $tpReg = $text | Select-String -Pattern "tp[\d]?[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})" -AllMatches;
    $slReg = $text | Select-String -Pattern "sl[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})";
    $priceReg = ($text | Select-String -Pattern "([0-9]{4}\.?[0-9]{0,2})");

    $price = [decimal]0;
    $sl = [decimal]0;
    $tps = New-Object Collections.Generic.List[MessageProps];

    $isSlTpOk = $slReg.Matches.Length -ne 0 -and $slReg.Matches[0].Groups.Length -ne 0 -and
    [decimal]::TryParse($slReg.Matches[0].Groups[1].Value.Replace(",", ".").Replace(" ", ""), $style, $culture, [ref]$sl);

    $isPriceOk = $priceReg.Matches.Length -ne 0 -and $priceReg.Matches[0].Groups.Length -ne 0 -and
    [decimal]::TryParse($priceReg.Matches[0].Groups[1].Value.Replace(",", ".").Replace(" ", ""), $style, $culture, [ref] $price);

    $tps = New-Object Collections.Generic.List[decimal];
    
    if ($isSlTpOk -and $tpReg.Matches.Length -ne 0 -and $tpReg.Matches[0].Groups.Length ) {
        $isTpOk = $false;
        foreach ($tpN in $tpReg.Matches) {
            $tpDecimal = [decimal]0;
            if ([decimal]::TryParse($tpN.Groups[1].Value.Replace(",", ".").Replace(" ", ""), $style, $culture, [ref] $tpDecimal)) {
                $tps.Add($tpDecimal);
                $isTpOk = $true;
            }
        }
        $isSlTpOk = $isTpOk;
    }

    if ($text -match "(buy)|(sell)[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})" -and $text -match $symbolLowerRegex) {

        if ($null -ne $lastSignal -and $lastSignal.IsSlTpDelayed) {
            # Write-Output "No TP or SL for " $item.text; 
            $signalsResult.Remove($lastSignal.Id);
        }
        
        if ($isPriceOk) {
            
            $lastSignal = [SignalProps]@{
                Id             = $item.id
                Price          = $price
                DatetimeUtc    = ([datetime]$item.date).ToUniversalTime().ToString("o")
                IsBuy          = [bool]($text -like "*buy*")
                MoveSlToProfit = New-Object Collections.Generic.List[MessageProps]
                TpHit          = New-Object Collections.Generic.List[MessageProps]
                IsSlTpDelayed  = $false;
            }

            if ($isSlTpOk) {
                $lastSignal.TakeProfits = $tps;
                $lastSignal.StopLoss = $sl;
            }
            else {
                $lastSignal.IsSlTpDelayed = $true;
            }

            $signalsResult.Add([string]$item.id, $lastSignal);
            continue;
        }

        # Write-Output "Cannot parse signal: " $text; 
    }

    if ($lastSignal.IsSlTpDelayed) {
        if ($isSlTpOk) {
            $lastSignal.IsSlTpDelayed = $false;
            $lastSignal.TakeProfits = $tps
            $lastSignal.StopLoss = $sl
            continue;

        }
        else {            
            # Write-Output "Cannot parse possible delayed TP/SL: " $text; 
        }
    }

    $lastSignalLocal = $lastSignal;
    $isReply = ($null -ne $item.reply_to_message_id) -and $signalsResult.ContainsKey($item.reply_to_message_id.ToString());

    if ($isReply) {        
        $lastSignalLocal = $signalsResult[$item.reply_to_message_id.ToString()];
    }
    
    $message = [MessageProps]@{
        Id          = $item.id
        IdRef       = $item.reply_to_message_id
        DatetimeUtc = ([datetime]$item.date).ToUniversalTime().ToString("o")
        #Text     = $text
    }

    if ((-not $isReply) -and (-not($text -match $symbolLowerRegex))) {
        #Write-Output "Cannot bind the message to a signal: " $text; 
        continue;        
    }

    if ($text.Length -gt $serviceMessageLengthMax) {
        #Write-Output "Too long service message, skip it: " $text; 
        continue;
    }

    if ($text -match "(book)|(entry point)|(breakeven)" -and (-not ($text -like "*hit*")) ) {
        
        $lastSignalLocal.MoveSlToEntry = $message;
        continue;
    }

    if ($text -like "*sl hit*") {
        $lastSignalLocal.SlHit = $message;
        continue;
    }

    if ($text -match "tp[0-4]? hit") {
        $lastSignalLocal.TpHit.Add($message);
        continue;
    }

    if ($text -match "move[\D]*sl[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})") {
        if ($isPriceOk) {
            $message.Price = $price
            $lastSignalLocal.MoveSlToProfit.Add($message);
            continue;
        }
        else {
            # Write-Output "Cannot move message: " $text;
        }
    }

    if ($text -match "(exit)|(close)") {        
        if ($isPriceOk) {
            $message.Price = $price
        }
        $lastSignalLocal.Exit = $message;
        continue;
    }
}

$count = $signalsResult.Count;
$outFile = ".\out.${channelName}.${symbolName}.json";
Write-Output "Total signals parsed: ${count}" ; 

$signalsResult.Values | Sort-Object -Property Id | ConvertTo-Json -depth 100 | Out-File $outFile;
Write-Output "Saved to: ${outFile}" ; 