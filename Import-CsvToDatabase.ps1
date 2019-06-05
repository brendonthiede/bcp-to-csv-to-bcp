function Import-JISDatabaseFromCsv {
    <#
  .SYNOPSIS
    Imports a CSV dataset into a database for DCS, CCS, PCS, or TCS
  .DESCRIPTION
    Imports a CSV dataset into a database for DCS, CCS, PCS, or TCS
  .PARAMETER ResourceGroupShortName
    The base name of the resource group, typically the same as the main service the resource group contains.
  .PARAMETER JISEnvironment
    Allows you to override the environment, i.e. to handle the monitoring subscription for Staging and Prod.
  #>
    [cmdletbinding()]
    [OutputType([String])]
    param(
        [Parameter(Mandatory = $False)]
        [String]
        $SourceFolder = "$PWD",

        [Parameter(Mandatory = $True)]
        [ValidateSet('TCS', 'CCS', 'PCS', 'DCS')]
        [String]
        $SchemaType,

        [Parameter(Mandatory = $False)]
        [String[]]
        $TablesToImport = @(),

        [Parameter(Mandatory = $False)]
        [String]
        $TargetServer = "(LocalDB)\MSSQLLocalDB",

        [Parameter(Mandatory = $False)]
        [String]
        $TargetDatabaseName = $SchemaType,

        [Parameter(Mandatory = $False)]
        [String]
        $TargetSchemaName = "dbo",

        [Parameter(Mandatory = $False)]
        [String]
        $RecordDelimiter = "!~!",
        
        [switch]
        $KeepBcpFiles = $false
    )

    if (-not (Get-Command bcp -ErrorAction SilentlyContinue)) {
        Write-Error "BCP is not installed." 
        Return
    }

    if (-not (Get-Command SQLCMD -ErrorAction SilentlyContinue)) {
        Write-Error "SQLCMD is not installed." 
        Return
    }

    $csvFiles = Get-ChildItem $SourceFolder/$SchemaType-*.csv
    foreach ($csvFile in $csvFiles) {
        $tableName = ($csvFile.BaseName -split "-")[1]
        Write-Verbose "Processing table : $tablename for schema : $schemaType"
        $bcpFileName = "$($csvFile.BaseName).bcp"
        Remove-Item $bcpFileName -Force -ErrorAction SilentlyContinue
      
        foreach ($row in (Import-Csv -Path $csvFile -Encoding ASCII)) {
            $row.psobject.Properties.value -JOIN "$RecordDelimiter" | Add-Content -Path $bcpFileName -Encoding ASCII       
        }

        SQLCMD.EXE -S $TargetServer -d $TargetDatabaseName -E -Q "TRUNCATE TABLE $($tablename)"

        bcp.exe "$TargetDatabaseName.$TargetSchemaName.$tableName" in $bcpFileName -S $TargetServer -T -c -CACP -t"$RecordDelimiter"

        if (-not($KeepBcpFiles)) {
            Remove-Item $bcpFileName -Force -ErrorAction SilentlyContinue
        }
    
    }
  
}
