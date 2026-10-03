(function(root){
 'use strict';
 function matches(outlet,method){
  const gdelt=outlet.estimated_country==='KE',cctld=outlet.domain_country==='KE';
  if(method==='gdelt')return gdelt;
  if(method==='cctld')return cctld;
  if(method==='both')return gdelt&&cctld;
  return false;
 }
 const api={matches};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.KenyaOrigins=api;
})(typeof window==='undefined'?globalThis:window);
