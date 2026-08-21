!macro NSIS_HOOK_POSTINSTALL
  ; Register app-specific ProgIDs without silently replacing the user's magnet default.
  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.Magnet" "" "URL:Magnet Protocol"
  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.Magnet" "URL Protocol" ""
  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.Magnet\DefaultIcon" "" "$INSTDIR\NovaTorrent.exe,0"
  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.Magnet\shell\open\command" "" '$\"$INSTDIR\NovaTorrent.exe$\" $\"%1$\"'

  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.NovaTorrent" "" "URL:NovaTorrent Protocol"
  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.NovaTorrent" "URL Protocol" ""
  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.NovaTorrent\DefaultIcon" "" "$INSTDIR\NovaTorrent.exe,0"
  WriteRegStr HKCU "Software\Classes\NovaTorrent.Url.NovaTorrent\shell\open\command" "" '$\"$INSTDIR\NovaTorrent.exe$\" $\"%1$\"'

  ; The private scheme can be activated immediately because NovaTorrent owns its namespace.
  WriteRegStr HKCU "Software\Classes\novatorrent" "" "URL:NovaTorrent Protocol"
  WriteRegStr HKCU "Software\Classes\novatorrent" "URL Protocol" ""
  WriteRegStr HKCU "Software\Classes\novatorrent\DefaultIcon" "" "$INSTDIR\NovaTorrent.exe,0"
  WriteRegStr HKCU "Software\Classes\novatorrent\shell\open\command" "" '$\"$INSTDIR\NovaTorrent.exe$\" $\"%1$\"'

  WriteRegStr HKCU "Software\NovaTorrent\Capabilities" "ApplicationName" "NovaTorrent"
  WriteRegStr HKCU "Software\NovaTorrent\Capabilities" "ApplicationDescription" "Modern BitTorrent client"
  WriteRegStr HKCU "Software\NovaTorrent\Capabilities" "ApplicationIcon" "$INSTDIR\NovaTorrent.exe,0"
  WriteRegStr HKCU "Software\NovaTorrent\Capabilities\UrlAssociations" "magnet" "NovaTorrent.Url.Magnet"
  WriteRegStr HKCU "Software\NovaTorrent\Capabilities\UrlAssociations" "novatorrent" "NovaTorrent.Url.NovaTorrent"
  WriteRegStr HKCU "Software\RegisteredApplications" "NovaTorrent" "Software\NovaTorrent\Capabilities"
!macroend

!macro NSIS_HOOK_POSTUNINSTALL
  DeleteRegValue HKCU "Software\RegisteredApplications" "NovaTorrent"
  DeleteRegKey HKCU "Software\NovaTorrent\Capabilities"
  DeleteRegKey /ifempty HKCU "Software\NovaTorrent"
  DeleteRegKey HKCU "Software\Classes\NovaTorrent.Url.Magnet"
  DeleteRegKey HKCU "Software\Classes\NovaTorrent.Url.NovaTorrent"
  DeleteRegKey HKCU "Software\Classes\novatorrent"
!macroend
