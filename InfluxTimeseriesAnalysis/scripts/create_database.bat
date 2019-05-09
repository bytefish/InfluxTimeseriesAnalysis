@echo off

:: Copyright (c) Philipp Wagner. All rights reserved.
:: Licensed under the MIT license. See LICENSE file in the project root for full license information.

set INFLUX_EXECUTABLE="G:\InfluxDB\influx.exe"
set STDOUT=stdout.log
set STDERR=stderr.log

set HostName=localhost
set PortNumber=8086
set DatabaseName=weather_data
set UserName=philipp
set Password=

call :AskQuestionWithYdefault "Use Host (%HostName%) Port (%PortNumber%) [Y,n]?" reply_
if /i [%reply_%] NEQ [y] (
	set /p HostName="Enter HostName: "
	set /p PortNumber="Enter Port: "
)

call :AskQuestionWithYdefault "Use Database (%DatabaseName%) [Y,n]?" reply_
if /i [%reply_%] NEQ [y]  (
	set /p ServerName="Enter Database: "
)

1>%STDOUT% 2>%STDERR% (
	%INFLUX_EXECUTABLE% -host %HostName% -port "%PortNumber%" -execute "CREATE DATABASE \"%DatabaseName%\" WITH DURATION inf REPLICATION 1 SHARD DURATION 4w NAME \"weather_data_policy\""
)

goto :end

:: The question as a subroutine
:AskQuestionWithYdefault
	setlocal enableextensions
	:_asktheyquestionagain
	set return_=
	set ask_=
	set /p ask_="%~1"
	if "%ask_%"=="" set return_=y
	if /i "%ask_%"=="Y" set return_=y
	if /i "%ask_%"=="n" set return_=n
	if not defined return_ goto _asktheyquestionagain
	endlocal & set "%2=%return_%" & goto :EOF

:end
pause