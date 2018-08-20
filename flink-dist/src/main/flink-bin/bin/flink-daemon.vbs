'::###############################################################################
'::  Licensed to the Apache Software Foundation (ASF) under one
'::  or more contributor license agreements.  See the NOTICE file
'::  distributed with this work for additional information
'::  regarding copyright ownership.  The ASF licenses this file
'::  to you under the Apache License, Version 2.0 (the
'::  "License"); you may not use this file except in compliance
'::  with the License.  You may obtain a copy of the License at
'::
'::      http://www.apache.org/licenses/LICENSE-2.0
'::
'::  Unless required by applicable law or agreed to in writing, software
'::  distributed under the License is distributed on an "AS IS" BASIS,
'::  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
'::  See the License for the specific language governing permissions and
':: limitations under the License.
'::###############################################################################

usage = "Usage: flink-daemon.bat (start|stop|stop-all) (jobmanager|taskmanager|historyserver|zookeeper|standalonesession) [args]"

If Wscript.Arguments.Count < 2 Then
    Wscript.Echo usage
    WScript.Quit 1
End If

startstop = WScript.Arguments.Item(0)
daemon = WScript.Arguments.Item(1)

Select case daemon
    case "jobmanager"
        CLASS_TO_RUN = "org.apache.flink.runtime.jobmanager.JobManager"
    case "taskmanager"
        CLASS_TO_RUN = "org.apache.flink.runtime.taskmanager.TaskManager"
    case "taskexecutor"
        CLASS_TO_RUN = "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"
    case "historyserver"
        CLASS_TO_RUN = "org.apache.flink.runtime.webmonitor.history.HistoryServer"
    case "zookeeper"
        CLASS_TO_RUN = "org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer"
    case "standalonesession"
        CLASS_TO_RUN = "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint"
    case else
        Wscript.Echo "Unknown daemon " & daemon & ". " & usage
        WScript.Quit 1
End select

Set Win32_Process = GetObject("winmgmts:Win32_Process")
Set fso = CreateObject("Scripting.FileSystemObject")
Set WMIService = GetObject("winmgmts:{impersonationLevel=impersonate}")
Set WScript_Shell = CreateObject( "WScript.Shell" )
Set Win32_ProcessStartup = GetObject("winmgmts:Win32_ProcessStartup")
strComputerName = WScript_Shell.ExpandEnvironmentStrings( "%COMPUTERNAME%" )

binDir = fso.GetParentFolderName(WScript.ScriptFullName)
FLINK_ROOT_DIR = fso.GetParentFolderName(binDir)
FLINK_LIB_DIR = FLINK_ROOT_DIR & "\lib"
FLINK_CLASSPATH = FLINK_LIB_DIR & "\*"
FLINK_CONF_DIR = FLINK_ROOT_DIR & "\conf"
FLINK_LOG_DIR = FLINK_ROOT_DIR & "\log"
JVM_ARGS = "-Xms1024m -Xmx1024m"


Select case startstop
    case "start"
        'Print a warning if daemons are already running on host
        Set colProcess = WMIService.ExecQuery("Select * from Win32_Process")

        count = 0
        For Each objProcess in colProcess
            if objProcess.Caption = "java.exe" And InStr(objProcess.CommandLine, CLASS_TO_RUN) Then
                count = count +1
                'Wscript.Echo objProcess.Caption & " | " & objProcess.processId & " | " & objProcess.CommandLine
            End If
        Next

        If count > 0 Then
            Wscript.Echo "[INFO] " & count & " instance(s) of " & daemon & " are already running on " & strComputerName & "."
        End If

        strUserName = WScript_Shell.ExpandEnvironmentStrings( "%USERNAME%" )
        logname = "flink-" & strUserName & "-taskmanager." & count & ".log"
        logfile = FLINK_LOG_DIR & "\" & logname
        outname = "flink-" & strUserName & "-taskmanager." & count & ".out"
        out = FLINK_LOG_DIR & "\" & outname
        log_setting = "-Dlog.file=""" & logfile & """ -Dlogback.configurationFile=file:""" & FLINK_CONF_DIR & "\logback.xml"" -Dlog4j.configuration=file:""" & FLINK_CONF_DIR & "\log4j.properties"""

        'logrotate
        For i = 5 to 0 Step -1
            j = i + 1
            fn = FLINK_LOG_DIR & "\" & logname & "." & i
            If fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & logname & "." & j
            fn = FLINK_LOG_DIR & "\" & outname & "." & i
            if fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & outname & "." & j
        Next
        fn = FLINK_LOG_DIR & "\" & logname
        if fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & logname & ".0"
        fn = FLINK_LOG_DIR & "\" & outname
        if fso.FileExists(fn) Then fso.MoveFile fn, FLINK_LOG_DIR & "\" & outname & ".0"
        fn = FLINK_LOG_DIR & "\" & logname & ".6"
        if fso.FileExists(fn) Then fso.DeleteFile fn
        fn = FLINK_LOG_DIR & "\" & outname & ".6"
        if fso.FileExists(fn) Then fso.DeleteFile fn

        Wscript.Echo "Starting " & daemon & " daemon on host " & strComputerName & "."
        Wscript.Echo "You can terminate the daemon with command flink-daemon.bat stop|stop-all."
        cmd = "cmd /c java " & JVM_ARGS & " " & log_setting & " -cp """ & FLINK_CLASSPATH & """; " & CLASS_TO_RUN & " --configDir """ & FLINK_CONF_DIR & """ "_
            & ARGS & " 1> """ & out & """ 2>&1"
        Set objConfig = Win32_ProcessStartup.SpawnInstance_
        objConfig.ShowWindow = 0
        If Win32_Process.Create(cmd,null,objConfig,processid) <> 0 Then Wscript.Echo "Error starting " & daemon & " daemon."

    case "stop"
        Set colProcess = WMIService.ExecQuery("Select * from Win32_Process")

        process = 0
        For Each objProcess in colProcess
            if objProcess.Caption = "java.exe" And InStr(objProcess.CommandLine, CLASS_TO_RUN) Then
                'Wscript.Echo objProcess.Caption & " | " & objProcess.processId & " | " & objProcess.CommandLine
                Set process = objProcess
                Exit For
            End If
        Next

        If Not IsObject(process) Then
            Wscript.Echo "No " & daemon & " daemon to stop on host " & strComputerName & "."
        Else
            Wscript.Echo "Stopping " & daemon & " daemon (pid: " & process.processId & ") on host " & strComputerName & "."
            process.Terminate
        End If

    case "stop-all"
        Set colProcess = WMIService.ExecQuery("Select * from Win32_Process")

        For Each objProcess in colProcess
            if objProcess.Caption = "java.exe" And InStr(objProcess.CommandLine, CLASS_TO_RUN) Then
                Wscript.Echo "Stopping " & daemon & " daemon (pid: " & objProcess.processId & ") on host " & strComputerName & "."
                'Wscript.Echo objProcess.Caption & " | " & objProcess.processId & " | " & objProcess.CommandLine
                objProcess.Terminate
            End If
        Next

    case else
        Wscript.Echo "Unexpected argument '" & startstop & "'. " & usage
        WScript.Quit 1

End select
