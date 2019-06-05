function Export-JISDatabaseToCsv {
    <#
  .SYNOPSIS
    Creates a CSV dataset from a database
  .DESCRIPTION
    Creates a CSV dataset from a database
  .PARAMETER ResourceGroupShortName
    The base name of the resource group, typically the same as the main service the resource group contains.
  #>
    [cmdletbinding()]
    [OutputType([String])]
    param(
        [Parameter(Mandatory = $False)]
        [String]
        $OutPath = "$PWD",

        [Parameter(Mandatory = $True)]
        [String]
        $SourceDatabaseName,

        [Parameter(Mandatory = $False)]
        [String[]]
        $TablesToExport = @(),

        [Parameter(Mandatory = $False)]
        [String]
        $SourceServer = "(LocalDB)\MSSQLLocalDB",

        [Parameter(Mandatory = $False)]
        [String]
        $SourceSchemaName = "dbo",

        [Parameter(Mandatory = $False)]
        [String]
        $RecordDelimiter = "!~!",

        [Parameter(Mandatory = $False)]
        [String]
        $ReadBatchSize = 1000,

        [switch]
        $KeepDmpFiles = $false
    )

    if (-not (Get-Command bcp -ErrorAction SilentlyContinue)) {
        Write-Error "BCP is not installed." 
        Return
    }
  
    foreach ($table in $TablesToExport) {
      
        $exportFileName = "$SourceDatabaseName-$table.dmp"
        $exportFileColumnsName = "$SourceDatabaseName-$($table)_columns.dmp"

        $Query = "Select Stuff(
              (
              Select '$RecordDelimiter' + C.name
              From $SourceDatabaseName.sys.COLUMNS As C
              Where c.object_id = t.object_id
              Order By c.column_id
              For Xml Path('')
              ), 1, 3, '') As Columns
      From $SourceDatabaseName.sys.TABLES As T
      WHERE t.NAME = '$table'"

        Write-Verbose "Exporting column names from $SourceServer.$SourceDatabaseName.$table to $exportFileColumnsName"
        bcp.exe $query queryout $exportFileColumnsName -S "$SourceServer" -T -c -t"$RecordDelimiter" -r\n
        Write-Verbose "Exporting data from $SourceServer.$SourceDatabaseName.$table to $exportFileName"
        bcp.exe "$SourceDatabaseName.$SourceSchemaName.$table" out $exportFileName  -S "$SourceServer" -T -c -t"$RecordDelimiter" -r\n
        Write-Verbose "Finished exporting data from $SourceServer.$SourceDatabaseName.$table to $exportFileName"
    }

    $columnHeaderFiles = Get-ChildItem $OutPath/*columns.dmp
    foreach ($columnHeaderFile in $columnHeaderFiles) {
        $SourceDatabaseName = ($columnHeaderFile.BaseName -split "-")[0]
        $tableName = (($columnHeaderFile.BaseName -split "-")[1] -split "_")[0]

        if (-not(Get-ChildItem $OutPath/"$SourceDatabaseName-$tableName.dmp" )) {
            Write-Verbose "Dump File $SourceDatabaseName-$tableName.dmp Does not Exists."
            return
        }

        $csvFileName = "$SourceDatabaseName-$tableName.csv"
        Remove-Item $csvFileName -Force -ErrorAction SilentlyContinue
        $columnHeaders = (Get-Content "$OutPath/$SourceDatabaseName-$($tableName)_columns.dmp" -Read 1) -split "$RecordDelimiter"
        $processedRows = 0
        Get-Content "$SourceDatabaseName-$tableName.dmp" -Read $ReadBatchSize -Encoding ASCII | ForEach-Object {
            $batchSize = $_.Count
            Write-Verbose "Reading batch of $batchSize rows from $SourceDatabaseName-$tableName.dmp"
            [System.Collections.ArrayList]$newCsvRecords = @()
            foreach ($row in $_) {
                $csvValues = $row -split "$RecordDelimiter"
                $csvRecord = [ordered]@{}
                for ($col = 0; $col -lt $columnHeaders.Count; $col++) {
                    $csvRecord += @{ $columnHeaders[$col] = $csvValues[$col] }
                }
                $newCsvRecords.Add([PSCustomObject]$csvRecord) | Out-Null
            }
            Write-Verbose "Writing batch of $batchSize rows to $csvFileName"
            $newCsvRecords | Export-Csv -Path $csvFileName -NoTypeInformation -Append -Encoding ASCII
            $processedRows += $batchSize
            Write-Verbose "$processedRows total rows processed"
        };

        if (-not($KeepDmpFiles)) {
            Remove-Item "$OutPath/$SourceDatabaseName-$tableName*.dmp" -Force -ErrorAction SilentlyContinue
        }
    }
}
