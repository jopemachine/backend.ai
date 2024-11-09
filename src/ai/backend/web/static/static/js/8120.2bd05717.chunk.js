/*! For license information please see 8120.2bd05717.chunk.js.LICENSE.txt */
"use strict";(self.webpackChunkbackend_ai_webui_react=self.webpackChunkbackend_ai_webui_react||[]).push([[8120],{48659:(e,t,n)=>{n.d(t,{A:()=>c});var l=n(40991),a=n(43373);const o={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M869 487.8L491.2 159.9c-2.9-2.5-6.6-3.9-10.5-3.9h-88.5c-7.4 0-10.8 9.2-5.2 14l350.2 304H152c-4.4 0-8 3.6-8 8v60c0 4.4 3.6 8 8 8h585.1L386.9 854c-5.6 4.9-2.2 14 5.2 14h91.5c1.9 0 3.8-.7 5.2-2L869 536.2a32.07 32.07 0 000-48.4z"}}]},name:"arrow-right",theme:"outlined"};var r=n(64098),i=function(e,t){return a.createElement(r.A,(0,l.A)({},e,{ref:t,icon:o}))};const c=a.forwardRef(i)},84197:(e,t,n)=>{n.d(t,{A:()=>c});var l=n(40991),a=n(43373);const o={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M168 504.2c1-43.7 10-86.1 26.9-126 17.3-41 42.1-77.7 73.7-109.4S337 212.3 378 195c42.4-17.9 87.4-27 133.9-27s91.5 9.1 133.8 27A341.5 341.5 0 01755 268.8c9.9 9.9 19.2 20.4 27.8 31.4l-60.2 47a8 8 0 003 14.1l175.7 43c5 1.2 9.9-2.6 9.9-7.7l.8-180.9c0-6.7-7.7-10.5-12.9-6.3l-56.4 44.1C765.8 155.1 646.2 92 511.8 92 282.7 92 96.3 275.6 92 503.8a8 8 0 008 8.2h60c4.4 0 7.9-3.5 8-7.8zm756 7.8h-60c-4.4 0-7.9 3.5-8 7.8-1 43.7-10 86.1-26.9 126-17.3 41-42.1 77.8-73.7 109.4A342.45 342.45 0 01512.1 856a342.24 342.24 0 01-243.2-100.8c-9.9-9.9-19.2-20.4-27.8-31.4l60.2-47a8 8 0 00-3-14.1l-175.7-43c-5-1.2-9.9 2.6-9.9 7.7l-.7 181c0 6.7 7.7 10.5 12.9 6.3l56.4-44.1C258.2 868.9 377.8 932 512.2 932c229.2 0 415.5-183.7 419.8-411.8a8 8 0 00-8-8.2z"}}]},name:"sync",theme:"outlined"};var r=n(64098),i=function(e,t){return a.createElement(r.A,(0,l.A)({},e,{ref:t,icon:o}))};const c=a.forwardRef(i)},41123:(e,t,n)=>{n.d(t,{A:()=>c});var l=n(40991),a=n(43373);const o={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M464 720a48 48 0 1096 0 48 48 0 10-96 0zm16-304v184c0 4.4 3.6 8 8 8h48c4.4 0 8-3.6 8-8V416c0-4.4-3.6-8-8-8h-48c-4.4 0-8 3.6-8 8zm475.7 440l-416-720c-6.2-10.7-16.9-16-27.7-16s-21.6 5.3-27.7 16l-416 720C56 877.4 71.4 904 96 904h832c24.6 0 40-26.6 27.7-48zm-783.5-27.9L512 239.9l339.8 588.2H172.2z"}}]},name:"warning",theme:"outlined"};var r=n(64098),i=function(e,t){return a.createElement(r.A,(0,l.A)({},e,{ref:t,icon:o}))};const c=a.forwardRef(i)},31409:(e,t,n)=>{n.d(t,{A:()=>i});var l=n(43373),a=n(46976),o=n(92685);var r=function(e){if(e.id,"undefined"===typeof cancelAnimationFrame)return clearInterval(e.id);cancelAnimationFrame(e.id)};const i=function(e,t,n){var i=null===n||void 0===n?void 0:n.immediate,c=(0,a.A)(e),s=(0,l.useRef)(),d=(0,l.useCallback)((function(){s.current&&r(s.current)}),[]);return(0,l.useEffect)((function(){if((0,o.Et)(t)&&!(t<0))return i&&c.current(),s.current=function(e,t){if(void 0===t&&(t=0),"undefined"===typeof requestAnimationFrame)return{id:setInterval(e,t)};var n=Date.now(),l={id:0},a=function(){Date.now()-n>=t&&(e(),n=Date.now()),l.id=requestAnimationFrame(a)};return l.id=requestAnimationFrame(a),l}((function(){c.current()}),t),d}),[t]),d}},86047:(e,t,n)=>{n.d(t,{A:()=>y});var l=n(43373),a=n(13001),o=n.n(a),r=n(92510),i=n(76633),c=n(45909),s=n(39067),d=n(67085),m=n(69056);const{Option:p}=m.A;function u(e){return(null===e||void 0===e?void 0:e.type)&&(e.type.isSelectOption||e.type.isSelectOptGroup)}const b=(e,t)=>{var n;const{prefixCls:a,className:s,popupClassName:b,dropdownClassName:f,children:g,dataSource:y}=e,h=(0,r.A)(g);let v;1===h.length&&l.isValidElement(h[0])&&!u(h[0])&&([v]=h);const $=v?()=>v:void 0;let O;O=h.length&&u(h[0])?g:y?y.map((e=>{if(l.isValidElement(e))return e;switch(typeof e){case"string":return l.createElement(p,{key:e,value:e},e);case"object":{const{value:t}=e;return l.createElement(p,{key:t,value:t},e.text)}default:return}})):[];const{getPrefixCls:w}=l.useContext(d.QO),x=w("select",a),[S]=(0,c.YK)("SelectLike",null===(n=e.dropdownStyle)||void 0===n?void 0:n.zIndex);return l.createElement(m.A,Object.assign({ref:t,suffixIcon:null},(0,i.A)(e,["dataSource","dropdownClassName"]),{prefixCls:x,popupClassName:b||f,dropdownStyle:Object.assign(Object.assign({},e.dropdownStyle),{zIndex:S}),className:o()(`${x}-auto-complete`,s),mode:m.A.SECRET_COMBOBOX_MODE_DO_NOT_USE,getInputElement:$}),O)},f=l.forwardRef(b),g=(0,s.A)(f);f.Option=p,f._InternalPanelDoNotUseOrYouWillBeFired=g;const y=f},45679:(e,t,n)=>{n.d(t,{A:()=>M});var l=n(43373),a=n(13001),o=n.n(a),r=n(98479),i=n(67085),c=n(19783),s=n(32597);const d={xxl:3,xl:3,lg:3,md:3,sm:2,xs:1},m=l.createContext({});var p=n(92510),u=function(e,t){var n={};for(var l in e)Object.prototype.hasOwnProperty.call(e,l)&&t.indexOf(l)<0&&(n[l]=e[l]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var a=0;for(l=Object.getOwnPropertySymbols(e);a<l.length;a++)t.indexOf(l[a])<0&&Object.prototype.propertyIsEnumerable.call(e,l[a])&&(n[l[a]]=e[l[a]])}return n};function b(e,t,n){const a=l.useMemo((()=>{return t||(e=n,(0,p.A)(e).map((e=>Object.assign(Object.assign({},null===e||void 0===e?void 0:e.props),{key:e.key}))));var e}),[t,n]);return l.useMemo((()=>a.map((t=>{var{span:n}=t,l=u(t,["span"]);return Object.assign(Object.assign({},l),{span:"number"===typeof n?n:(0,r.ko)(e,n)})}))),[a,e])}function f(e,t,n){let l=e,a=!1;return(void 0===n||n>t)&&(l=Object.assign(Object.assign({},e),{span:t}),a=void 0!==n),[l,a]}const g=(e,t)=>{const[n,a]=(0,l.useMemo)((()=>function(e,t){const n=[];let l=[],a=t,o=!1;return e.filter((e=>e)).forEach(((r,i)=>{const c=null===r||void 0===r?void 0:r.span,s=c||1;if(i===e.length-1){const[e,t]=f(r,a,c);return o=o||t,l.push(e),void n.push(l)}if(s<a)a-=s,l.push(r);else{const[e,i]=f(r,a,s);o=o||i,l.push(e),n.push(l),a=t,l=[]}})),[n,o]}(t,e)),[t,e]);return n},y=e=>{let{children:t}=e;return t};function h(e){return void 0!==e&&null!==e}const v=e=>{const{itemPrefixCls:t,component:n,span:a,className:r,style:i,labelStyle:c,contentStyle:s,bordered:d,label:m,content:p,colon:u,type:b}=e,f=n;return d?l.createElement(f,{className:o()({[`${t}-item-label`]:"label"===b,[`${t}-item-content`]:"content"===b},r),style:i,colSpan:a},h(m)&&l.createElement("span",{style:c},m),h(p)&&l.createElement("span",{style:s},p)):l.createElement(f,{className:o()(`${t}-item`,r),style:i,colSpan:a},l.createElement("div",{className:`${t}-item-container`},(m||0===m)&&l.createElement("span",{className:o()(`${t}-item-label`,{[`${t}-item-no-colon`]:!u}),style:c},m),(p||0===p)&&l.createElement("span",{className:o()(`${t}-item-content`),style:s},p)))};function $(e,t,n){let{colon:a,prefixCls:o,bordered:r}=t,{component:i,type:c,showLabel:s,showContent:d,labelStyle:m,contentStyle:p}=n;return e.map(((e,t)=>{let{label:n,children:u,prefixCls:b=o,className:f,style:g,labelStyle:y,contentStyle:h,span:$=1,key:O}=e;return"string"===typeof i?l.createElement(v,{key:`${c}-${O||t}`,className:f,style:g,labelStyle:Object.assign(Object.assign({},m),y),contentStyle:Object.assign(Object.assign({},p),h),span:$,colon:a,component:i,itemPrefixCls:b,bordered:r,label:s?n:null,content:d?u:null,type:c}):[l.createElement(v,{key:`label-${O||t}`,className:f,style:Object.assign(Object.assign(Object.assign({},m),g),y),span:1,colon:a,component:i[0],itemPrefixCls:b,bordered:r,label:n,type:"label"}),l.createElement(v,{key:`content-${O||t}`,className:f,style:Object.assign(Object.assign(Object.assign({},p),g),h),span:2*$-1,component:i[1],itemPrefixCls:b,bordered:r,content:u,type:"content"})]}))}const O=e=>{const t=l.useContext(m),{prefixCls:n,vertical:a,row:o,index:r,bordered:i}=e;return a?l.createElement(l.Fragment,null,l.createElement("tr",{key:`label-${r}`,className:`${n}-row`},$(o,e,Object.assign({component:"th",type:"label",showLabel:!0},t))),l.createElement("tr",{key:`content-${r}`,className:`${n}-row`},$(o,e,Object.assign({component:"td",type:"content",showContent:!0},t)))):l.createElement("tr",{key:r,className:`${n}-row`},$(o,e,Object.assign({component:i?["th","td"]:"td",type:"item",showLabel:!0,showContent:!0},t)))};var w=n(87),x=n(10751),S=n(64188),A=n(8299);const E=e=>{const{componentCls:t,labelBg:n}=e;return{[`&${t}-bordered`]:{[`> ${t}-view`]:{overflow:"hidden",border:`${(0,w.zA)(e.lineWidth)} ${e.lineType} ${e.colorSplit}`,"> table":{tableLayout:"auto"},[`${t}-row`]:{borderBottom:`${(0,w.zA)(e.lineWidth)} ${e.lineType} ${e.colorSplit}`,"&:last-child":{borderBottom:"none"},[`> ${t}-item-label, > ${t}-item-content`]:{padding:`${(0,w.zA)(e.padding)} ${(0,w.zA)(e.paddingLG)}`,borderInlineEnd:`${(0,w.zA)(e.lineWidth)} ${e.lineType} ${e.colorSplit}`,"&:last-child":{borderInlineEnd:"none"}},[`> ${t}-item-label`]:{color:e.colorTextSecondary,backgroundColor:n,"&::after":{display:"none"}}}},[`&${t}-middle`]:{[`${t}-row`]:{[`> ${t}-item-label, > ${t}-item-content`]:{padding:`${(0,w.zA)(e.paddingSM)} ${(0,w.zA)(e.paddingLG)}`}}},[`&${t}-small`]:{[`${t}-row`]:{[`> ${t}-item-label, > ${t}-item-content`]:{padding:`${(0,w.zA)(e.paddingXS)} ${(0,w.zA)(e.padding)}`}}}}}},C=(0,S.OF)("Descriptions",(e=>(e=>{const{componentCls:t,extraColor:n,itemPaddingBottom:l,itemPaddingEnd:a,colonMarginRight:o,colonMarginLeft:r,titleMarginBottom:i}=e;return{[t]:Object.assign(Object.assign(Object.assign({},(0,x.dF)(e)),E(e)),{"&-rtl":{direction:"rtl"},[`${t}-header`]:{display:"flex",alignItems:"center",marginBottom:i},[`${t}-title`]:Object.assign(Object.assign({},x.L9),{flex:"auto",color:e.titleColor,fontWeight:e.fontWeightStrong,fontSize:e.fontSizeLG,lineHeight:e.lineHeightLG}),[`${t}-extra`]:{marginInlineStart:"auto",color:n,fontSize:e.fontSize},[`${t}-view`]:{width:"100%",borderRadius:e.borderRadiusLG,table:{width:"100%",tableLayout:"fixed",borderCollapse:"collapse"}},[`${t}-row`]:{"> th, > td":{paddingBottom:l,paddingInlineEnd:a},"> th:last-child, > td:last-child":{paddingInlineEnd:0},"&:last-child":{borderBottom:"none","> th, > td":{paddingBottom:0}}},[`${t}-item-label`]:{color:e.colorTextTertiary,fontWeight:"normal",fontSize:e.fontSize,lineHeight:e.lineHeight,textAlign:"start","&::after":{content:'":"',position:"relative",top:-.5,marginInline:`${(0,w.zA)(r)} ${(0,w.zA)(o)}`},[`&${t}-item-no-colon::after`]:{content:'""'}},[`${t}-item-no-label`]:{"&::after":{margin:0,content:'""'}},[`${t}-item-content`]:{display:"table-cell",flex:1,color:e.contentColor,fontSize:e.fontSize,lineHeight:e.lineHeight,wordBreak:"break-word",overflowWrap:"break-word"},[`${t}-item`]:{paddingBottom:0,verticalAlign:"top","&-container":{display:"flex",[`${t}-item-label`]:{display:"inline-flex",alignItems:"baseline"},[`${t}-item-content`]:{display:"inline-flex",alignItems:"baseline",minWidth:0}}},"&-middle":{[`${t}-row`]:{"> th, > td":{paddingBottom:e.paddingSM}}},"&-small":{[`${t}-row`]:{"> th, > td":{paddingBottom:e.paddingXS}}}})}})((0,A.oX)(e,{}))),(e=>({labelBg:e.colorFillAlter,titleColor:e.colorText,titleMarginBottom:e.fontSizeSM*e.lineHeightSM,itemPaddingBottom:e.padding,itemPaddingEnd:e.padding,colonMarginRight:e.marginXS,colonMarginLeft:e.marginXXS/2,contentColor:e.colorText,extraColor:e.colorText})));var k=function(e,t){var n={};for(var l in e)Object.prototype.hasOwnProperty.call(e,l)&&t.indexOf(l)<0&&(n[l]=e[l]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var a=0;for(l=Object.getOwnPropertySymbols(e);a<l.length;a++)t.indexOf(l[a])<0&&Object.prototype.propertyIsEnumerable.call(e,l[a])&&(n[l[a]]=e[l[a]])}return n};const j=e=>{const{prefixCls:t,title:n,extra:a,column:p,colon:u=!0,bordered:f,layout:y,children:h,className:v,rootClassName:$,style:w,size:x,labelStyle:S,contentStyle:A,items:E}=e,j=k(e,["prefixCls","title","extra","column","colon","bordered","layout","children","className","rootClassName","style","size","labelStyle","contentStyle","items"]),{getPrefixCls:M,direction:z,descriptions:N}=l.useContext(i.QO),B=M("descriptions",t),I=(0,s.A)(),L=l.useMemo((()=>{var e;return"number"===typeof p?p:null!==(e=(0,r.ko)(I,Object.assign(Object.assign({},d),p)))&&void 0!==e?e:3}),[I,p]),P=b(I,E,h),_=(0,c.A)(x),T=g(L,P),[F,H,R]=C(B),W=l.useMemo((()=>({labelStyle:S,contentStyle:A})),[S,A]);return F(l.createElement(m.Provider,{value:W},l.createElement("div",Object.assign({className:o()(B,null===N||void 0===N?void 0:N.className,{[`${B}-${_}`]:_&&"default"!==_,[`${B}-bordered`]:!!f,[`${B}-rtl`]:"rtl"===z},v,$,H,R),style:Object.assign(Object.assign({},null===N||void 0===N?void 0:N.style),w)},j),(n||a)&&l.createElement("div",{className:`${B}-header`},n&&l.createElement("div",{className:`${B}-title`},n),a&&l.createElement("div",{className:`${B}-extra`},a)),l.createElement("div",{className:`${B}-view`},l.createElement("table",null,l.createElement("tbody",null,T.map(((e,t)=>l.createElement(O,{key:t,index:t,colon:u,prefixCls:B,vertical:"vertical"===y,bordered:f,row:e})))))))))};j.Item=y;const M=j},3681:(e,t,n)=>{n.d(t,{A:()=>l});const l=(0,n(43106).A)("BotMessageSquare",[["path",{d:"M12 6V2H8",key:"1155em"}],["path",{d:"m8 18-4 4V8a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2Z",key:"w2lp3e"}],["path",{d:"M2 12h2",key:"1t8f8n"}],["path",{d:"M9 11v2",key:"1ueba0"}],["path",{d:"M15 11v2",key:"i11awn"}],["path",{d:"M20 12h2",key:"1q8mjw"}]])},8652:(e,t,n)=>{n.d(t,{A:()=>l});const l=(0,n(43106).A)("Microchip",[["path",{d:"M18 12h2",key:"quuxs7"}],["path",{d:"M18 16h2",key:"zsn3lv"}],["path",{d:"M18 20h2",key:"9x5y9y"}],["path",{d:"M18 4h2",key:"1luxfb"}],["path",{d:"M18 8h2",key:"nxqzg"}],["path",{d:"M4 12h2",key:"1ltxp0"}],["path",{d:"M4 16h2",key:"8a5zha"}],["path",{d:"M4 20h2",key:"27dk57"}],["path",{d:"M4 4h2",key:"10groj"}],["path",{d:"M4 8h2",key:"18vq6w"}],["path",{d:"M8 2a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2h-1.5c-.276 0-.494.227-.562.495a2 2 0 0 1-3.876 0C9.994 2.227 9.776 2 9.5 2z",key:"1681fp"}]])}}]);
//# sourceMappingURL=8120.2bd05717.chunk.js.map