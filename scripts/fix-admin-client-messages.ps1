# Regenerate admin_client_messages.html
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$out = Join-Path $root "app\templates\admin_client_messages.html"
$users = Join-Path $root "app\templates\admin_users.html"
$lines = Get-Content -LiteralPath $users -Encoding UTF8
$nav = ($lines[76..87] -join "`n")
$tail = Get-Content -LiteralPath $out -Encoding UTF8 | Select-Object -Skip 82
$d = -join @('d','i','v')
$bodyCard = @"
      <$d class="card">
        <h4 style="margin:0 0 10px;">&#21069;&#21488;&#30041;&#35328;&#65288;&#32852;&#31995;&#25105;&#20204; / &#24847;&#35265;&#24314;&#35758;&#65289;</h4>
        <$d class="row">
          <label>&#31867;&#22411;</label>
          <select id="kindSel">
            <option value="">&#20840;&#37096;</option>
            <option value="contact">&#32852;&#31995;&#25105;&#20204;</option>
            <option value="feedback">&#24847;&#35265;&#24314;&#35758;</option>
          </select>
          <button onclick="page=1;loadMessages()">&#26597;&#35810;</button>
          <span id="msg" class="muted"></span>
        </$d>
        <$d style="overflow-x:auto;margin-top:10px;">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>&#31867;&#22411;</th>
                <th>&#26631;&#39064;</th>
                <th>&#20869;&#23481;</th>
                <th>&#32852;&#31995;&#26041;&#24335;</th>
                <th>&#29992;&#25143;</th>
                <th>&#35775;&#23458;</th>
                <th>IP</th>
                <th>&#26102;&#38388;</th>
              </tr>
            </thead>
            <tbody id="tb"></tbody>
          </table>
        </$d>
        <$d class="pager">
          <button class="secondary" id="btnFirst" onclick="goFirst()">&#39318;&#39029;</button>
          <button class="secondary" id="btnPrev" onclick="goPrev()">&#19978;&#19968;&#39029;</button>
          <button class="secondary" id="btnNext" onclick="goNext()">&#19979;&#19968;&#39029;</button>
          <span id="pagerInfo" class="muted"></span>
        </$d>
      </$d>
"@
$head = @'
<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{{ app_name }} - &#30041;&#35328;&#31649;&#29702;</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 0; background: #f5f7fb; color: #1b2430; }
      .container { max-width: 1200px; margin: 0 auto; padding: 16px; }
      .card { background: #fff; border-radius: 10px; padding: 12px; margin-bottom: 12px; box-shadow: 0 2px 10px rgba(20,33,61,.08); }
      .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
      button, select { padding: 8px 10px; border: 1px solid #ccd3de; border-radius: 8px; }
      button { background: #2463eb; color: #fff; border: none; cursor: pointer; }
      button.secondary { background: #667085; }
      .muted { color: #667085; }
      .nav a { margin-right: 12px; color: #2463eb; text-decoration: none; font-weight: 600; }
      .row.nav { width: 100%; box-sizing: border-box; background: #edf3ff; border: 1px solid #dbe6ff; }
      button.logout-soft {
        margin-left: auto; padding: 6px 12px; border-radius: 8px; border: 1px solid #e2e8f0;
        background: #f1f5f9; color: #94a3b8; font-size: 13px; cursor: pointer;
      }
      table { width: 100%; border-collapse: collapse; font-size: 13px; }
      th, td { border-bottom: 1px solid #e9eef5; padding: 8px; text-align: left; vertical-align: top; }
      td.content-cell { max-width: 360px; white-space: normal; word-break: break-word; }
      .kind-contact { color: #2463eb; font-weight: 600; }
      .kind-feedback { color: #7c3aed; font-weight: 600; }
      .pager { display: flex; gap: 8px; align-items: center; margin-top: 10px; flex-wrap: wrap; }
    </style>
  </head>
  <body>
    <div class="container">
'@
$html = $head + "`n" + $nav + "`n" + $bodyCard + "`n    </$d>`n" + ($tail -join "`n")
[System.IO.File]::WriteAllText((Resolve-Path $out).Path, $html, (New-Object System.Text.UTF8Encoding $false))
Write-Host "OK"
