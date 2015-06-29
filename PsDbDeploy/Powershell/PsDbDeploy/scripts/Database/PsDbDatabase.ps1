class PsDbDatabase
{
    $DatabaseProvider = "Undefined"
    $DefaultCommandTimeout = 100


    hidden [System.Data.Common.DbConnection] $_connection

    # defined in abstract.  However, because it is used to track the necessity
    # of applying updates to the PsDbDeploy schema, the value is managed by the
    # Database specific class
    [int] $PsDbDeployVersion

    [string] ConnectionInfo()
    {
        $sb = [System.Text.StringBuilder]::new() 
        $sb.AppendLine("  DatabaseProvider: $($this.DatabaseProvider)")
        $sb.AppendLine(" PsDbDeployVersion: $($this.PsDbDeployVersion)")
        $sb.AppendLine("        ServerName: $($this._connection.DataSource)")
        $sb.AppendLine("      DatabaseName: $($this._connection.Database)")
        $sb.AppendLine("  Connection State: $($this._connection.State)")

        return $sb.ToString()
    }

    hidden [System.Data.Common.DbCommand] _newSqlCommand()
    {
        [System.Data.Common.DbCommand]$NewSqlCmd = $this._connection.CreateCommand()

        $NewSqlCmd.CommandTimeout = $this.DefaultCommandTimeout
        $NewSqlCmd.CommandType = [System.Data.CommandType]::Text

        return $NewSqlCmd
    }

    hidden [System.Data.Common.DbCommand] _newSqlCommand($CommandText='')
    {
        $NewSqlCmd = $this._newSqlCommand()

        $NewSqlCmd.CommandText = $CommandText

        return $NewSqlCmd
    }


    hidden _AssurePsDbDeploy([int]$PsDbDeployVersion)
    {
        if ($PsDbDeployVersion -gt $this.PsDbDeployVersion)
        {
            $this.ExecutePsDbDeployUpdate($PsDbDeployVersion)
        }
    }

    ExecutePsDbDeployUpdate([int]$PsDbDeployVersion)
    {
        throw "Not Implemented"
    }

    hidden [int] _getPsDbDeployVersion()
    {
        $SqlCommand = $this._newSqlCommand($this._GetPsDbDeployVersionSql)
        $version = $SqlCommand.ExecuteScalar()
        return $version
    }

    LogExecutedSql($query)
    {

    }

    [bool] $PublishWhatIf = $false
    [bool] $DisplayCallStack = $true

    TerminalError($Exception,$OptionalMsg)
    {
        $sb = [System.Text.StringBuilder]::new() 

        $ExceptionMessage = $Exception.Exception.Message;
        if ($Exception.Exception.InnerException)
        {
            $ExceptionMessage = $Exception.Exception.InnerException.Message;
        }
        $errorQueryMsg = "`n{0}`n{1}" -f $ExceptionMessage,$OptionalMsg
        $sb.AppendLine("$errorQueryMsg")

    
        if ($this.DisplayCallStack)
        {
            $brkline = '=========================================================================='
            $sb.AppendLine("$brkline")
            $sb.AppendLine("$errorQueryMsg")
            $sb.AppendLine('Stack calls')
            $sb.AppendLine("$brkline")

            $stack = Get-PSCallStack

            $sb.AppendLine("Location: $($Exception.InvocationInfo.PositionMessage)")
            $sb.AppendLine(" Command: $($stack[1].Command)")
            #$sb.AppendLine("Position: $($Exception.InvocationInfo.Line)")
            $sb.AppendLine($brkline)

            for ($i = 1; $i -lt $stack.Count; $i++)
            #foreach ($stackItem in $stack)
            {
                $stackItem = $stack[$i]
                $sb.AppendLine("Location: $($stackItem.Location)")
                $sb.AppendLine(" Command: $($stackItem.Command)")
                $sb.AppendLine("Position: $($stackItem.Position)")
                $sb.AppendLine($brkline)
            }
        }
        Write-Output $sb.ToString()
        Exit
    }

    [string[]] ParseSqlStrings ($SqlStrings)
    {
        $SqlString = $SqlStrings | Out-String

        $results = @()

        $SqlQueries = $this._queriesRegex.Matches($SqlString)
        foreach ($capture in $SqlQueries[0].Groups["Query"].Captures)
        {
            $results += $capture.Value | ?{($_).trim().Length -gt 0}  # don't return empty strings
        }
        return $results
    }


    ExecuteNonQuery($Query)
    {
        $cmd = $this._newSqlCommand()

        $ParsedQueries = $this.ParseSqlStrings($Query)
        foreach ($ParsedQuery in $ParsedQueries)
        {
            if ($ParsedQuery.Trim() -ne "")
            {
                $this.LogExecutedSql($ParsedQuery)
                if (! $this.PublishWhatIf)
                {
                    try
                    {
                        $cmd.CommandText=$ParsedQuery
                        [void] $cmd.ExecuteNonQuery()
                    } 
                    catch
                    {
                        $this.TerminalError($_,$ParsedQuery)
                    }
                }
            }
        }
    }
}

