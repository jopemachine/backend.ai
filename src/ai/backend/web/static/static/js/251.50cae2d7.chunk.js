(self.webpackChunkbackend_ai_webui_react=self.webpackChunkbackend_ai_webui_react||[]).push([[251],{67318:(e,t,n)=>{"use strict";n.d(t,{A:()=>c});var o=n(40991),r=n(43373);const l={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M696 480H328c-4.4 0-8 3.6-8 8v48c0 4.4 3.6 8 8 8h368c4.4 0 8-3.6 8-8v-48c0-4.4-3.6-8-8-8z"}},{tag:"path",attrs:{d:"M512 64C264.6 64 64 264.6 64 512s200.6 448 448 448 448-200.6 448-448S759.4 64 512 64zm0 820c-205.4 0-372-166.6-372-372s166.6-372 372-372 372 166.6 372 372-166.6 372-372 372z"}}]},name:"minus-circle",theme:"outlined"};var i=n(64098),a=function(e,t){return r.createElement(i.A,(0,o.A)({},e,{ref:t,icon:l}))};const c=r.forwardRef(a)},27250:(e,t,n)=>{"use strict";n.d(t,{A:()=>c});var o=n(40991),r=n(43373);const l={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M872 474H152c-4.4 0-8 3.6-8 8v60c0 4.4 3.6 8 8 8h720c4.4 0 8-3.6 8-8v-60c0-4.4-3.6-8-8-8z"}}]},name:"minus",theme:"outlined"};var i=n(64098),a=function(e,t){return r.createElement(i.A,(0,o.A)({},e,{ref:t,icon:l}))};const c=r.forwardRef(a)},88319:(e,t,n)=>{"use strict";n.d(t,{A:()=>u});var o=n(81949),r=n(82073),l=n.n(r),i=n(43373),a=n(46976),c=n(35649),s=n(92685),d=n(77418);const u=function(e,t){var n;d.A&&((0,s.Tn)(e)||console.error("useThrottleFn expected parameter is a function, got ".concat(typeof e)));var r=(0,a.A)(e),u=null!==(n=null===t||void 0===t?void 0:t.wait)&&void 0!==n?n:1e3,m=(0,i.useMemo)((function(){return l()((function(){for(var e=[],t=0;t<arguments.length;t++)e[t]=arguments[t];return r.current.apply(r,(0,o.fX)([],(0,o.zs)(e),!1))}),u,t)}),[]);return(0,c.A)((function(){m.cancel()})),{run:m,cancel:m.cancel,flush:m.flush}}},45679:(e,t,n)=>{"use strict";n.d(t,{A:()=>z});var o=n(43373),r=n(13001),l=n.n(r),i=n(98479),a=n(67085),c=n(19783),s=n(32597);const d={xxl:3,xl:3,lg:3,md:3,sm:2,xs:1},u=o.createContext({});var m=n(92510),p=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};function f(e,t,n){const r=o.useMemo((()=>{return t||(e=n,(0,m.A)(e).map((e=>Object.assign(Object.assign({},null===e||void 0===e?void 0:e.props),{key:e.key}))));var e}),[t,n]);return o.useMemo((()=>r.map((t=>{var{span:n}=t,o=p(t,["span"]);return Object.assign(Object.assign({},o),{span:"number"===typeof n?n:(0,i.ko)(e,n)})}))),[r,e])}function b(e,t,n){let o=e,r=!1;return(void 0===n||n>t)&&(o=Object.assign(Object.assign({},e),{span:t}),r=void 0!==n),[o,r]}const g=(e,t)=>{const[n,r]=(0,o.useMemo)((()=>function(e,t){const n=[];let o=[],r=t,l=!1;return e.filter((e=>e)).forEach(((i,a)=>{const c=null===i||void 0===i?void 0:i.span,s=c||1;if(a===e.length-1){const[e,t]=b(i,r,c);return l=l||t,o.push(e),void n.push(o)}if(s<r)r-=s,o.push(i);else{const[e,a]=b(i,r,s);l=l||a,o.push(e),n.push(o),r=t,o=[]}})),[n,l]}(t,e)),[t,e]);return n},y=e=>{let{children:t}=e;return t};function v(e){return void 0!==e&&null!==e}const h=e=>{const{itemPrefixCls:t,component:n,span:r,className:i,style:a,labelStyle:c,contentStyle:s,bordered:d,label:u,content:m,colon:p,type:f}=e,b=n;return d?o.createElement(b,{className:l()({[`${t}-item-label`]:"label"===f,[`${t}-item-content`]:"content"===f},i),style:a,colSpan:r},v(u)&&o.createElement("span",{style:c},u),v(m)&&o.createElement("span",{style:s},m)):o.createElement(b,{className:l()(`${t}-item`,i),style:a,colSpan:r},o.createElement("div",{className:`${t}-item-container`},(u||0===u)&&o.createElement("span",{className:l()(`${t}-item-label`,{[`${t}-item-no-colon`]:!p}),style:c},u),(m||0===m)&&o.createElement("span",{className:l()(`${t}-item-content`),style:s},m)))};function $(e,t,n){let{colon:r,prefixCls:l,bordered:i}=t,{component:a,type:c,showLabel:s,showContent:d,labelStyle:u,contentStyle:m}=n;return e.map(((e,t)=>{let{label:n,children:p,prefixCls:f=l,className:b,style:g,labelStyle:y,contentStyle:v,span:$=1,key:x}=e;return"string"===typeof a?o.createElement(h,{key:`${c}-${x||t}`,className:b,style:g,labelStyle:Object.assign(Object.assign({},u),y),contentStyle:Object.assign(Object.assign({},m),v),span:$,colon:r,component:a,itemPrefixCls:f,bordered:i,label:s?n:null,content:d?p:null,type:c}):[o.createElement(h,{key:`label-${x||t}`,className:b,style:Object.assign(Object.assign(Object.assign({},u),g),y),span:1,colon:r,component:a[0],itemPrefixCls:f,bordered:i,label:n,type:"label"}),o.createElement(h,{key:`content-${x||t}`,className:b,style:Object.assign(Object.assign(Object.assign({},m),g),v),span:2*$-1,component:a[1],itemPrefixCls:f,bordered:i,content:p,type:"content"})]}))}const x=e=>{const t=o.useContext(u),{prefixCls:n,vertical:r,row:l,index:i,bordered:a}=e;return r?o.createElement(o.Fragment,null,o.createElement("tr",{key:`label-${i}`,className:`${n}-row`},$(l,e,Object.assign({component:"th",type:"label",showLabel:!0},t))),o.createElement("tr",{key:`content-${i}`,className:`${n}-row`},$(l,e,Object.assign({component:"td",type:"content",showContent:!0},t)))):o.createElement("tr",{key:i,className:`${n}-row`},$(l,e,Object.assign({component:a?["th","td"]:"td",type:"item",showLabel:!0,showContent:!0},t)))};var O=n(87),j=n(10751),S=n(64188),w=n(8299);const E=e=>{const{componentCls:t,labelBg:n}=e;return{[`&${t}-bordered`]:{[`> ${t}-view`]:{overflow:"hidden",border:`${(0,O.zA)(e.lineWidth)} ${e.lineType} ${e.colorSplit}`,"> table":{tableLayout:"auto"},[`${t}-row`]:{borderBottom:`${(0,O.zA)(e.lineWidth)} ${e.lineType} ${e.colorSplit}`,"&:last-child":{borderBottom:"none"},[`> ${t}-item-label, > ${t}-item-content`]:{padding:`${(0,O.zA)(e.padding)} ${(0,O.zA)(e.paddingLG)}`,borderInlineEnd:`${(0,O.zA)(e.lineWidth)} ${e.lineType} ${e.colorSplit}`,"&:last-child":{borderInlineEnd:"none"}},[`> ${t}-item-label`]:{color:e.colorTextSecondary,backgroundColor:n,"&::after":{display:"none"}}}},[`&${t}-middle`]:{[`${t}-row`]:{[`> ${t}-item-label, > ${t}-item-content`]:{padding:`${(0,O.zA)(e.paddingSM)} ${(0,O.zA)(e.paddingLG)}`}}},[`&${t}-small`]:{[`${t}-row`]:{[`> ${t}-item-label, > ${t}-item-content`]:{padding:`${(0,O.zA)(e.paddingXS)} ${(0,O.zA)(e.padding)}`}}}}}},C=(0,S.OF)("Descriptions",(e=>(e=>{const{componentCls:t,extraColor:n,itemPaddingBottom:o,itemPaddingEnd:r,colonMarginRight:l,colonMarginLeft:i,titleMarginBottom:a}=e;return{[t]:Object.assign(Object.assign(Object.assign({},(0,j.dF)(e)),E(e)),{"&-rtl":{direction:"rtl"},[`${t}-header`]:{display:"flex",alignItems:"center",marginBottom:a},[`${t}-title`]:Object.assign(Object.assign({},j.L9),{flex:"auto",color:e.titleColor,fontWeight:e.fontWeightStrong,fontSize:e.fontSizeLG,lineHeight:e.lineHeightLG}),[`${t}-extra`]:{marginInlineStart:"auto",color:n,fontSize:e.fontSize},[`${t}-view`]:{width:"100%",borderRadius:e.borderRadiusLG,table:{width:"100%",tableLayout:"fixed",borderCollapse:"collapse"}},[`${t}-row`]:{"> th, > td":{paddingBottom:o,paddingInlineEnd:r},"> th:last-child, > td:last-child":{paddingInlineEnd:0},"&:last-child":{borderBottom:"none","> th, > td":{paddingBottom:0}}},[`${t}-item-label`]:{color:e.colorTextTertiary,fontWeight:"normal",fontSize:e.fontSize,lineHeight:e.lineHeight,textAlign:"start","&::after":{content:'":"',position:"relative",top:-.5,marginInline:`${(0,O.zA)(i)} ${(0,O.zA)(l)}`},[`&${t}-item-no-colon::after`]:{content:'""'}},[`${t}-item-no-label`]:{"&::after":{margin:0,content:'""'}},[`${t}-item-content`]:{display:"table-cell",flex:1,color:e.contentColor,fontSize:e.fontSize,lineHeight:e.lineHeight,wordBreak:"break-word",overflowWrap:"break-word"},[`${t}-item`]:{paddingBottom:0,verticalAlign:"top","&-container":{display:"flex",[`${t}-item-label`]:{display:"inline-flex",alignItems:"baseline"},[`${t}-item-content`]:{display:"inline-flex",alignItems:"baseline",minWidth:0}}},"&-middle":{[`${t}-row`]:{"> th, > td":{paddingBottom:e.paddingSM}}},"&-small":{[`${t}-row`]:{"> th, > td":{paddingBottom:e.paddingXS}}}})}})((0,w.oX)(e,{}))),(e=>({labelBg:e.colorFillAlter,titleColor:e.colorText,titleMarginBottom:e.fontSizeSM*e.lineHeightSM,itemPaddingBottom:e.padding,itemPaddingEnd:e.padding,colonMarginRight:e.marginXS,colonMarginLeft:e.marginXXS/2,contentColor:e.colorText,extraColor:e.colorText})));var A=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};const N=e=>{const{prefixCls:t,title:n,extra:r,column:m,colon:p=!0,bordered:b,layout:y,children:v,className:h,rootClassName:$,style:O,size:j,labelStyle:S,contentStyle:w,items:E}=e,N=A(e,["prefixCls","title","extra","column","colon","bordered","layout","children","className","rootClassName","style","size","labelStyle","contentStyle","items"]),{getPrefixCls:z,direction:k,descriptions:M}=o.useContext(a.QO),T=z("descriptions",t),B=(0,s.A)(),P=o.useMemo((()=>{var e;return"number"===typeof m?m:null!==(e=(0,i.ko)(B,Object.assign(Object.assign({},d),m)))&&void 0!==e?e:3}),[B,m]),I=f(B,E,v),L=(0,c.A)(j),W=g(P,I),[H,X,F]=C(T),R=o.useMemo((()=>({labelStyle:S,contentStyle:w})),[S,w]);return H(o.createElement(u.Provider,{value:R},o.createElement("div",Object.assign({className:l()(T,null===M||void 0===M?void 0:M.className,{[`${T}-${L}`]:L&&"default"!==L,[`${T}-bordered`]:!!b,[`${T}-rtl`]:"rtl"===k},h,$,X,F),style:Object.assign(Object.assign({},null===M||void 0===M?void 0:M.style),O)},N),(n||r)&&o.createElement("div",{className:`${T}-header`},n&&o.createElement("div",{className:`${T}-title`},n),r&&o.createElement("div",{className:`${T}-extra`},r)),o.createElement("div",{className:`${T}-view`},o.createElement("table",null,o.createElement("tbody",null,W.map(((e,t)=>o.createElement(x,{key:t,index:t,colon:p,prefixCls:T,vertical:"vertical"===y,bordered:b,row:e})))))))))};N.Item=y;const z=N},2412:(e,t,n)=>{var o=n(81952).Symbol;e.exports=o},98025:(e,t,n)=>{var o=n(2412),r=n(63168),l=n(39047),i=o?o.toStringTag:void 0;e.exports=function(e){return null==e?void 0===e?"[object Undefined]":"[object Null]":i&&i in Object(e)?r(e):l(e)}},93485:(e,t,n)=>{var o=n(28343),r=/^\s+/;e.exports=function(e){return e?e.slice(0,o(e)+1).replace(r,""):e}},47769:(e,t,n)=>{var o="object"==typeof n.g&&n.g&&n.g.Object===Object&&n.g;e.exports=o},63168:(e,t,n)=>{var o=n(2412),r=Object.prototype,l=r.hasOwnProperty,i=r.toString,a=o?o.toStringTag:void 0;e.exports=function(e){var t=l.call(e,a),n=e[a];try{e[a]=void 0;var o=!0}catch(c){}var r=i.call(e);return o&&(t?e[a]=n:delete e[a]),r}},39047:e=>{var t=Object.prototype.toString;e.exports=function(e){return t.call(e)}},81952:(e,t,n)=>{var o=n(47769),r="object"==typeof self&&self&&self.Object===Object&&self,l=o||r||Function("return this")();e.exports=l},28343:e=>{var t=/\s/;e.exports=function(e){for(var n=e.length;n--&&t.test(e.charAt(n)););return n}},88182:(e,t,n)=>{var o=n(36230),r=n(29437),l=n(57001),i=Math.max,a=Math.min;e.exports=function(e,t,n){var c,s,d,u,m,p,f=0,b=!1,g=!1,y=!0;if("function"!=typeof e)throw new TypeError("Expected a function");function v(t){var n=c,o=s;return c=s=void 0,f=t,u=e.apply(o,n)}function h(e){var n=e-p;return void 0===p||n>=t||n<0||g&&e-f>=d}function $(){var e=r();if(h(e))return x(e);m=setTimeout($,function(e){var n=t-(e-p);return g?a(n,d-(e-f)):n}(e))}function x(e){return m=void 0,y&&c?v(e):(c=s=void 0,u)}function O(){var e=r(),n=h(e);if(c=arguments,s=this,p=e,n){if(void 0===m)return function(e){return f=e,m=setTimeout($,t),b?v(e):u}(p);if(g)return clearTimeout(m),m=setTimeout($,t),v(p)}return void 0===m&&(m=setTimeout($,t)),u}return t=l(t)||0,o(n)&&(b=!!n.leading,d=(g="maxWait"in n)?i(l(n.maxWait)||0,t):d,y="trailing"in n?!!n.trailing:y),O.cancel=function(){void 0!==m&&clearTimeout(m),f=0,c=p=s=m=void 0},O.flush=function(){return void 0===m?u:x(r())},O}},36230:e=>{e.exports=function(e){var t=typeof e;return null!=e&&("object"==t||"function"==t)}},35041:e=>{e.exports=function(e){return null!=e&&"object"==typeof e}},54633:(e,t,n)=>{var o=n(98025),r=n(35041);e.exports=function(e){return"symbol"==typeof e||r(e)&&"[object Symbol]"==o(e)}},29437:(e,t,n)=>{var o=n(81952);e.exports=function(){return o.Date.now()}},82073:(e,t,n)=>{var o=n(88182),r=n(36230);e.exports=function(e,t,n){var l=!0,i=!0;if("function"!=typeof e)throw new TypeError("Expected a function");return r(n)&&(l="leading"in n?!!n.leading:l,i="trailing"in n?!!n.trailing:i),o(e,t,{leading:l,maxWait:t,trailing:i})}},57001:(e,t,n)=>{var o=n(93485),r=n(36230),l=n(54633),i=/^[-+]0x[0-9a-f]+$/i,a=/^0b[01]+$/i,c=/^0o[0-7]+$/i,s=parseInt;e.exports=function(e){if("number"==typeof e)return e;if(l(e))return NaN;if(r(e)){var t="function"==typeof e.valueOf?e.valueOf():e;e=r(t)?t+"":t}if("string"!=typeof e)return 0===e?e:+e;e=o(e);var n=a.test(e);return n||c.test(e)?s(e.slice(2),n?2:8):i.test(e)?NaN:+e}}}]);
//# sourceMappingURL=251.50cae2d7.chunk.js.map