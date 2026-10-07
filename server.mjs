import http from 'node:http';
import {spawn} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {mkdir,readFile,readdir,rm,stat,writeFile} from 'node:fs/promises';
import {createReadStream} from 'node:fs';
import {lookup} from 'node:dns/promises';
import net from 'node:net';
import path from 'node:path';

const PORT=Number(process.env.PORT||3000);
const ROOT=process.env.JOB_ROOT||'/data/jobs';
const TTL=60*60*1000;
const CHUNK_SECONDS=120;
const WHISPER_BIN=process.env.WHISPER_BIN||'/usr/local/bin/whisper-cli';
const WHISPER_MODEL=process.env.WHISPER_MODEL||'/opt/whisper/models/ggml-tiny.bin';
const WHISPER_THREADS=String(Math.max(1,Math.min(4,Number(process.env.WHISPER_THREADS||2)||2)));

await mkdir(ROOT,{recursive:true});
const jobs=new Map();
const queue=[];
let workerBusy=false;

async function exists(file){try{await stat(file);return true}catch{return false}}
function chunkRuntime(j,index){
  const n=String(index).padStart(4,'0');
  return {
    audioFile:path.join(j.chunksDir,`chunk-${n}.wav`),
    prefix:path.join(j.chunksDir,`part-${n}`),
    transcriptFile:path.join(j.chunksDir,`part-${n}.txt`)
  };
}
function hydrateSegment(j,s){return {...s,...chunkRuntime(j,s.index)}}
function completedSegments(j){return (j.segments||[]).filter(s=>s.status==='done')}
function combinedText(j){
  return completedSegments(j).map(s=>`=== ${clock(s.startSec)} - ${clock(s.endSec)} ===\n${s.text||''}`.trim()).join('\n\n');
}
function clock(sec){const n=Math.max(0,Math.floor(Number(sec)||0)),m=Math.floor(n/60),s=n%60;return `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`}

async function saveJob(j){
  const data={
    id:j.id,input:j.input,clientId:j.clientId,status:j.status,stage:j.stage,message:j.message||'',
    createdAt:j.createdAt,wavReady:!!j.wavReady,wavReadyAt:j.wavReadyAt||null,sizeMB:j.sizeMB||null,
    chunksReady:!!j.chunksReady,segments:(j.segments||[]).map(s=>({
      index:s.index,status:s.status,startSec:s.startSec,endSec:s.endSec,text:s.text||'',completedAt:s.completedAt||null
    })),
    transcript:j.transcript||'',expires:j.expires||null,error:j.error||''
  };
  await writeFile(path.join(j.dir,'job.json'),JSON.stringify(data),'utf8');
}

async function restoreJobs(){
  for(const id of await readdir(ROOT).catch(()=>[])){
    const dir=path.join(ROOT,id);
    try{
      const d=JSON.parse(await readFile(path.join(dir,'job.json'),'utf8'));
      if(d.expires&&d.expires<=Date.now()){await rm(dir,{recursive:true,force:true});continue}
      const j={
        ...d,dir,file:path.join(dir,'audio.wav'),chunksDir:path.join(dir,'chunks'),
        transcriptFile:path.join(dir,'transcript.txt')
      };
      j.wavReady=!!d.wavReady&&await exists(j.file);
      if(!d.wavReady&&await exists(j.file))j.wavReady=true;
      j.segments=(d.segments||[]).map(s=>hydrateSegment(j,s));
      if(['queued','running'].includes(j.status)){
        j.status='queued';j.stage='queued';j.message='서버 재시작 후 다시 처리 대기 중';j.expires=null;
        for(const s of j.segments)if(s.status==='running')s.status='pending';
        queue.push(id);
      }
      jobs.set(id,j);
    }catch{}
  }
}
await restoreJobs();

setInterval(async()=>{
  const now=Date.now();
  for(const [id,j] of jobs){
    if(j.expires&&j.expires<=now){jobs.delete(id);await rm(j.dir,{recursive:true,force:true})}
  }
},30000).unref();

function json(res,code,obj){res.writeHead(code,{'content-type':'application/json; charset=utf-8','cache-control':'no-store'});res.end(JSON.stringify(obj))}
function cleanError(e){return String(e?.message||e||'알 수 없는 오류').replace(/[\r\n]+/g,' ').slice(0,1200)}
function privateIP(ip){
  if(net.isIPv4(ip)){
    const p=ip.split('.').map(Number);
    return p[0]===10||p[0]===127||p[0]===0||p[0]===169&&p[1]===254||p[0]===172&&p[1]>=16&&p[1]<=31||p[0]===192&&p[1]===168;
  }
  return ip==='::1'||ip.startsWith('fc')||ip.startsWith('fd')||ip.startsWith('fe80:');
}
async function safeUrl(raw){
  let u;try{u=new URL(raw)}catch{throw Error('올바른 URL이 아닙니다.')}
  if(!['http:','https:'].includes(u.protocol)||u.username||u.password)throw Error('http/https URL만 허용됩니다.');
  const addrs=await lookup(u.hostname,{all:true});
  if(!addrs.length||addrs.some(x=>privateIP(x.address)))throw Error('내부 네트워크 주소는 허용되지 않습니다.');
  return u;
}
function extractM3u8(text){
  const decoded=String(text).replaceAll('\\/','/');
  const re=/https?:\/\/[^\s"'<>]+?\.m3u8(?:\?[^\s"'<>]*)?/ig;
  return decoded.match(re)?.[0]||null;
}
async function fetchText(url){
  const u=await safeUrl(url);
  const r=await fetch(u,{redirect:'manual',signal:AbortSignal.timeout(12000),headers:{'user-agent':'jstowav/1.4'}});
  if(r.status>=300&&r.status<400&&r.headers.get('location'))return fetchText(new URL(r.headers.get('location'),u).href);
  if(!r.ok)throw Error(`원격 서버 HTTP ${r.status}`);
  const b=await r.arrayBuffer();
  if(b.byteLength>2_000_000)throw Error('페이지/플레이리스트가 너무 큽니다.');
  return {text:new TextDecoder().decode(b),url:r.url||u.href};
}
async function discover(raw){
  const pasted=extractM3u8(raw);
  if(pasted)return (await safeUrl(pasted)).href;
  const u=await safeUrl(raw.trim());
  if(u.pathname.toLowerCase().endsWith('.m3u8'))return u.href;
  const {text,url}=await fetchText(u.href);
  const found=extractM3u8(text);
  if(found)return (await safeUrl(new URL(found,url).href)).href;
  const rel=[...text.matchAll(/["']([^"']+\.m3u8(?:\?[^"']*)?)["']/ig)];
  for(const m of rel){try{return (await safeUrl(new URL(m[1],url).href)).href}catch{}}
  throw Error('페이지/텍스트에서 m3u8 링크를 찾지 못했습니다.');
}
function run(cmd,args,{timeout=180000,label='작업'}={}){
  return new Promise((resolve,reject)=>{
    const p=spawn(cmd,args,{stdio:['ignore','ignore','pipe']});
    let e='';
    const t=setTimeout(()=>{p.kill('SIGKILL');reject(Error(`${label} 시간 제한을 초과했습니다.`))},timeout);
    p.stderr.on('data',d=>{if(e.length<12000)e+=d});
    p.on('error',err=>{clearTimeout(t);reject(Error(`${label} 실행 실패: ${err.message}`))});
    p.on('close',c=>{clearTimeout(t);c===0?resolve():reject(Error(`${label} 실패: ${e.slice(-800)}`))});
  });
}
async function body(req){
  let s='';for await(const c of req){s+=c;if(s.length>100000)throw Error('요청이 너무 큽니다.')}
  return JSON.parse(s||'{}');
}
async function writeCombined(j){
  j.transcript=combinedText(j);
  if(j.transcript)await writeFile(j.transcriptFile,j.transcript,'utf8');
}
function jobView(j){
  const done=completedSegments(j).length;
  const out={
    id:j.id,status:j.status,stage:j.stage,message:j.message||'',createdAt:j.createdAt,
    wavReady:!!j.wavReady,sizeMB:j.sizeMB||null,segmentCount:(j.segments||[]).length,completedSegments:done,
    segments:(j.segments||[]).map(s=>({
      index:s.index+1,status:s.status,startSec:s.startSec,endSec:s.endSec,
      transcriptDownload:s.status==='done'?`/transcript/${j.id}/${s.index+1}`:null
    }))
  };
  if(j.wavReady)out.download='/download/'+j.id;
  if(done)out.transcriptDownload='/transcript/'+j.id;
  if(j.expires)out.expiresIn=Math.max(0,Math.ceil((j.expires-Date.now())/1000));
  if(j.status==='failed')out.error=j.error;
  return out;
}

async function processJob(j){
  j.status='running';j.error='';await saveJob(j);
  try{
    if(!j.wavReady){
      j.stage='discover';j.message='m3u8 탐색 중';await saveJob(j);
      const m3u8=await discover(j.input),mu=await safeUrl(m3u8);
      j.stage='wav';j.message='WAV 변환 중';await saveJob(j);
      await run('ffmpeg',['-nostdin','-hide_banner','-loglevel','error','-protocol_whitelist','file,http,https,tcp,tls,crypto','-i',mu.href,'-vn','-acodec','pcm_s16le','-ar','44100','-ac','2','-y',j.file],{label:'WAV 변환'});
      const st=await stat(j.file);
      if(st.size>500_000_000)throw Error('생성 파일이 너무 큽니다.');
      j.sizeMB=(st.size/1048576).toFixed(1);j.wavReady=true;j.wavReadyAt=new Date().toISOString();
      j.stage='split';j.message='WAV 생성 완료 · 지금 다운로드할 수 있습니다. 2분 단위 분할 중';await saveJob(j);
    }else if(!await exists(j.file)){
      throw Error('보관된 WAV 파일을 찾을 수 없습니다.');
    }

    if(!j.chunksReady){
      j.stage='split';j.message='WAV 다운로드 가능 · 전사용 오디오를 2분 단위로 분할 중';await saveJob(j);
      await rm(j.chunksDir,{recursive:true,force:true});await mkdir(j.chunksDir,{recursive:true});
      await run('ffmpeg',['-nostdin','-hide_banner','-loglevel','error','-i',j.file,'-vn','-acodec','pcm_s16le','-ar','16000','-ac','1','-f','segment','-segment_time',String(CHUNK_SECONDS),'-reset_timestamps','1','-y',path.join(j.chunksDir,'chunk-%04d.wav')],{timeout:300000,label:'2분 단위 분할'});
      const files=(await readdir(j.chunksDir)).filter(x=>/^chunk-\d{4}\.wav$/.test(x)).sort();
      if(!files.length)throw Error('전사용 오디오 조각을 만들지 못했습니다.');
      j.segments=files.map((name,index)=>hydrateSegment(j,{index,status:'pending',startSec:index*CHUNK_SECONDS,endSec:(index+1)*CHUNK_SECONDS,text:'',completedAt:null}));
      j.chunksReady=true;await saveJob(j);
    }

    for(let i=0;i<j.segments.length;i++){
      const seg=j.segments[i];
      if(seg.status==='done'&&await exists(seg.transcriptFile))continue;
      if(!await exists(seg.audioFile))throw Error(`${i+1}번 전사용 오디오 조각을 찾을 수 없습니다.`);
      seg.status='running';j.stage='transcribe';j.message=`WAV 다운로드 가능 · 영어 전사 ${i+1}/${j.segments.length} (각 2분)`;await saveJob(j);
      await run(WHISPER_BIN,['-m',WHISPER_MODEL,'-f',seg.audioFile,'-l','auto','-tr','-nt','-otxt','-of',seg.prefix,'-t',WHISPER_THREADS],{timeout:1800000,label:`영어 전사 ${i+1}/${j.segments.length}`});
      seg.text=(await readFile(seg.transcriptFile,'utf8')).trim();seg.status='done';seg.completedAt=new Date().toISOString();
      await rm(seg.audioFile,{force:true});await writeCombined(j);await saveJob(j);
    }

    await writeCombined(j);
    j.status='done';j.stage='done';j.message='WAV 및 2분 단위 영어 전사 완료';j.expires=Date.now()+TTL;await saveJob(j);
  }catch(e){
    j.status='failed';j.stage='failed';j.message='처리 실패';j.error=cleanError(e);j.expires=Date.now()+TTL;
    await writeCombined(j).catch(()=>{});await saveJob(j).catch(()=>{});
  }
}
async function drainQueue(){
  if(workerBusy)return;workerBusy=true;
  try{while(queue.length){const id=queue.shift(),j=jobs.get(id);if(j&&j.status==='queued')await processJob(j)}}
  finally{workerBusy=false;if(queue.length)setImmediate(drainQueue)}
}
setImmediate(drainQueue);

const page=`<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>JS to WAV + English Transcript</title><style>body{font-family:system-ui;background:#0b1020;color:#eef2ff;margin:0}.w{max-width:820px;margin:5vh auto;padding:24px}.card{background:#151b31;padding:28px;border-radius:20px;box-shadow:0 20px 60px #0006}h1{margin-top:0}textarea,button{box-sizing:border-box;width:100%;padding:15px;border-radius:12px;border:1px solid #39415e;font-size:15px}textarea{min-height:220px;resize:vertical;background:#0e1428;color:white;font-family:ui-monospace,monospace}button{margin-top:12px;background:#eef2ff;color:#111827;font-weight:700;cursor:pointer}.muted{color:#aab3cf;font-size:14px}.out{margin-top:18px;white-space:pre-wrap}.jobs{margin-top:24px}.job{padding:14px 0;border-top:1px solid #303852}.job small{color:#aab3cf}.links{margin-top:7px;line-height:1.8}.parts{margin-top:6px;font-size:14px}.progress{margin-top:5px;color:#c7d2fe;font-size:14px}a{color:#9fc1ff}</style><div class="w"><div class="card"><h1>JS to WAV + English Transcript</h1><p>페이지 URL, m3u8 URL 또는 Network/Response 로그 전체를 그대로 붙여넣으세요.</p><textarea id="u" placeholder="GET https://…/2000k.m3u8 200\n\nResponse Headers\n…\n#EXTM3U\n…"></textarea><button id="b">WAV 변환 + 영어 텍스트 전사</button><div class="out" id="o"></div><div class="jobs"><h2>최근 작업</h2><div id="jobs" class="muted">불러오는 중…</div></div><p class="muted">WAV가 만들어지는 즉시 다운로드할 수 있습니다. 영어 전사는 2분 단위로 순차 처리되며 완료된 조각부터 TXT로 받을 수 있습니다. 작업이 끝난 뒤 다운로드 파일은 1시간 보관됩니다. 본인이 이용 권한이 있는 미디어에만 사용하세요. DRM/접근통제 우회는 지원하지 않습니다.</p></div></div><script>
const h=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const clientId=localStorage.jstowavClientId||(localStorage.jstowavClientId=crypto.randomUUID());
const u=document.getElementById('u'),b=document.getElementById('b'),o=document.getElementById('o'),jobsEl=document.getElementById('jobs');
const tm=s=>{s=Math.max(0,Number(s)||0);const m=Math.floor(s/60),x=Math.floor(s%60);return String(m).padStart(2,'0')+':'+String(x).padStart(2,'0')};
async function getJson(url,opt={},retries=2){opt.headers={...(opt.headers||{}),'x-client-id':clientId};let last;for(let i=0;i<=retries;i++){try{const r=await fetch(url,opt),j=await r.json();if(!r.ok)throw Error(j.error||('HTTP '+r.status));return j}catch(e){last=e;if(i<retries)await wait(1200)}}throw last}
function jobHtml(j){
  const st=j.status==='done'?'완료':j.status==='failed'?'실패':(j.message||j.status);
  const wav=j.wavReady?'<a href="'+h(j.download)+'">WAV 다운로드'+(j.sizeMB?' ('+h(j.sizeMB)+' MB)':'')+'</a>':'';
  const combined=j.transcriptDownload?'<a href="'+h(j.transcriptDownload)+'">완료된 전사 합본 TXT</a>':'';
  const links=[wav,combined].filter(Boolean).join(' · ');
  const parts=(j.segments||[]).filter(x=>x.status==='done').map(x=>'<a href="'+h(x.transcriptDownload)+'">'+h(x.index)+'번 '+tm(x.startSec)+'-'+tm(x.endSec)+' TXT</a>').join(' · ');
  const progress=j.segmentCount?'<div class="progress">전사 완료 '+h(j.completedSegments)+' / '+h(j.segmentCount)+' 조각</div>':'';
  const ttl=j.expiresIn!=null?'<div class="muted">다운로드 약 '+Math.max(1,Math.ceil(j.expiresIn/60))+'분 남음</div>':'<div class="muted">처리 완료 후 1시간 보관</div>';
  return '<div class="job"><strong>'+h(st)+'</strong> <small>'+h(new Date(j.createdAt).toLocaleString())+'</small>'+progress+(links?'<div class="links">'+links+'</div>':'')+(parts?'<div class="parts">조각: '+parts+'</div>':'')+(j.status==='failed'?'<div>오류: '+h(j.error||'처리 실패')+'</div>':'')+ttl+'</div>';
}
async function refreshJobs(){try{const data=await getJson('/api/jobs',{},1);jobsEl.innerHTML=data.jobs.length?data.jobs.map(jobHtml).join(''):'이 브라우저에서 접수한 보관 작업이 없습니다.'}catch{jobsEl.textContent='작업 목록을 불러오지 못했습니다.'}}
function currentHtml(j){const links=[];if(j.wavReady)links.push('<a href="'+h(j.download)+'">WAV 바로 다운로드</a>');if(j.transcriptDownload)links.push('<a href="'+h(j.transcriptDownload)+'">현재까지 전사 합본 TXT</a>');const parts=(j.segments||[]).filter(x=>x.status==='done').map(x=>'<a href="'+h(x.transcriptDownload)+'">'+x.index+'번 조각 TXT</a>');return h(j.message||'처리 중…')+(j.segmentCount?' · '+j.completedSegments+'/'+j.segmentCount+' 조각 완료':'')+(links.length?'<br>'+links.join(' · '):'')+(parts.length?'<br>완료 조각: '+parts.join(' · '):'')}
b.onclick=async()=>{b.disabled=true;o.textContent='요청 접수 중…';try{const start=await getJson('/api/convert',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({input:u.value})});for(;;){await wait(1800);const j=await getJson('/api/jobs/'+encodeURIComponent(start.id),{},3);o.innerHTML=currentHtml(j);await refreshJobs();if(j.status==='failed')throw Error(j.error||'처리 실패');if(j.status==='done')break}}catch(e){o.textContent='오류: '+(e?.message||e)}finally{b.disabled=false;refreshJobs()}};
refreshJobs();setInterval(refreshJobs,3500);
</script></html>`;

function artifactActive(j){return !j.expires||j.expires>Date.now()}
function sendFile(res,file,type,name){return stat(file).then(st=>{res.writeHead(200,{'content-type':type,'content-length':st.size,'content-disposition':`attachment; filename="${name}"`,'cache-control':'private, no-store'});createReadStream(file).pipe(res)})}

const server=http.createServer(async(req,res)=>{
  try{
    const u=new URL(req.url,'http://localhost');
    const clientId=String(req.headers['x-client-id']||'').slice(0,100);
    if(req.method==='GET'&&u.pathname==='/'){res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});return res.end(page)}
    if(req.method==='GET'&&u.pathname==='/api/health')return json(res,200,{status:'ok',transcription:'local-whisper',mode:'chunked-transcription',retentionSeconds:3600,transcriptChunkSeconds:CHUNK_SECONDS});
    if(req.method==='GET'&&u.pathname==='/api/jobs'){
      if(!clientId)return json(res,200,{jobs:[]});
      return json(res,200,{jobs:[...jobs.values()].filter(j=>j.clientId===clientId).sort((a,b)=>String(b.createdAt).localeCompare(String(a.createdAt))).map(jobView)});
    }
    if(req.method==='POST'&&u.pathname==='/api/convert'){
      if(!clientId)throw Error('브라우저 작업 ID가 없습니다.');
      const b=await body(req),input=b.input??b.url;
      if(typeof input!=='string'||!input.trim())throw Error('URL 또는 로그 텍스트를 입력하세요.');
      const id=randomUUID(),dir=path.join(ROOT,id);
      await mkdir(dir,{mode:0o700});
      const j={id,input,clientId,dir,file:path.join(dir,'audio.wav'),chunksDir:path.join(dir,'chunks'),transcriptFile:path.join(dir,'transcript.txt'),status:'queued',stage:'queued',message:'처리 대기 중',createdAt:new Date().toISOString(),wavReady:false,wavReadyAt:null,sizeMB:null,chunksReady:false,segments:[],transcript:'',expires:null,error:''};
      jobs.set(id,j);await saveJob(j);queue.push(id);setImmediate(drainQueue);
      return json(res,202,{id,status:'queued',statusUrl:'/api/jobs/'+id});
    }
    if(req.method==='GET'&&u.pathname.startsWith('/api/jobs/')){
      const id=u.pathname.slice('/api/jobs/'.length),j=jobs.get(id);
      if(!j||j.clientId!==clientId)return json(res,404,{error:'작업이 만료되었거나 존재하지 않습니다.'});
      return json(res,200,jobView(j));
    }
    if(req.method==='GET'&&u.pathname.startsWith('/download/')){
      const id=u.pathname.slice('/download/'.length),j=jobs.get(id);
      if(!j||!j.wavReady||!artifactActive(j)||!await exists(j.file))return json(res,404,{error:'WAV가 아직 준비되지 않았거나 보관 시간이 만료되었습니다.'});
      return await sendFile(res,j.file,'audio/wav','audio.wav');
    }
    if(req.method==='GET'&&u.pathname.startsWith('/transcript/')){
      const parts=u.pathname.slice('/transcript/'.length).split('/').filter(Boolean),j=jobs.get(parts[0]);
      if(!j||!artifactActive(j))return json(res,404,{error:'전사 파일이 아직 준비되지 않았거나 보관 시간이 만료되었습니다.'});
      if(parts.length===1){
        if(!completedSegments(j).length||!await exists(j.transcriptFile))return json(res,404,{error:'완료된 전사 조각이 아직 없습니다.'});
        return await sendFile(res,j.transcriptFile,'text/plain; charset=utf-8','transcript-en-completed.txt');
      }
      const n=Number(parts[1]),seg=Number.isInteger(n)&&n>=1?j.segments?.[n-1]:null;
      if(!seg||seg.status!=='done'||!await exists(seg.transcriptFile))return json(res,404,{error:'해당 전사 조각이 아직 준비되지 않았습니다.'});
      return await sendFile(res,seg.transcriptFile,'text/plain; charset=utf-8',`transcript-en-part-${String(n).padStart(3,'0')}.txt`);
    }
    return json(res,404,{error:'not found'});
  }catch(e){return json(res,400,{error:cleanError(e)})}
});
server.listen(PORT,'0.0.0.0',()=>console.log(`jstowav listening ${PORT}`));
