"use strict";(self.webpackChunkbackend_ai_webui_react=self.webpackChunkbackend_ai_webui_react||[]).push([[498],{41814:(e,t,n)=>{n.d(t,{A:()=>A});var o=n(76998),r=n(34156),l=n.n(r),a=n(30324),c=n(52787),i=n(96556),s=n(28037),p=n(51749),m=n(37998);const u=e=>{let{children:t}=e;const{getPrefixCls:n}=o.useContext(s.QO),r=n("breadcrumb");return o.createElement("li",{className:"".concat(r,"-separator"),"aria-hidden":"true"},""===t?t:t||"/")};u.__ANT_BREADCRUMB_SEPARATOR=!0;const d=u;var f=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};function b(e,t,n,r){if(null===n||void 0===n)return null;const{className:a,onClick:i}=t,s=f(t,["className","onClick"]),p=Object.assign(Object.assign({},(0,c.A)(s,{data:!0,aria:!0})),{onClick:i});return void 0!==r?o.createElement("a",Object.assign({},p,{className:l()("".concat(e,"-link"),a),href:r}),n):o.createElement("span",Object.assign({},p,{className:l()("".concat(e,"-link"),a)}),n)}function g(e,t){return(n,o,r,l,a)=>{if(t)return t(n,o,r,l);const c=function(e,t){if(void 0===e.title||null===e.title)return null;const n=Object.keys(t).join("|");return"object"===typeof e.title?e.title:String(e.title).replace(new RegExp(":(".concat(n,")"),"g"),((e,n)=>t[n]||e))}(n,o);return b(e,n,c,a)}}var O=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};const y=e=>{const{prefixCls:t,separator:n="/",children:r,menu:l,overlay:a,dropdownProps:c,href:i}=e;const s=(e=>{if(l||a){const n=Object.assign({},c);if(l){const e=l||{},{items:t}=e,r=O(e,["items"]);n.menu=Object.assign(Object.assign({},r),{items:null===t||void 0===t?void 0:t.map(((e,t)=>{var{key:n,title:r,label:l,path:a}=e,c=O(e,["key","title","label","path"]);let s=null!==l&&void 0!==l?l:r;return a&&(s=o.createElement("a",{href:"".concat(i).concat(a)},s)),Object.assign(Object.assign({},c),{key:null!==n&&void 0!==n?n:t,label:s})}))})}else a&&(n.overlay=a);return o.createElement(m.A,Object.assign({placement:"bottom"},n),o.createElement("span",{className:"".concat(t,"-overlay-link")},e,o.createElement(p.A,null)))}return e})(r);return void 0!==s&&null!==s?o.createElement(o.Fragment,null,o.createElement("li",null,s),n&&o.createElement(d,null,n)):null},v=e=>{const{prefixCls:t,children:n,href:r}=e,l=O(e,["prefixCls","children","href"]),{getPrefixCls:a}=o.useContext(s.QO),c=a("breadcrumb",t);return o.createElement(y,Object.assign({},l,{prefixCls:c}),b(c,l,n,r))};v.__ANT_BREADCRUMB_ITEM=!0;const C=v;var h=n(89213),x=n(64663),j=n(55864),k=n(89514);const E=(0,j.OF)("Breadcrumb",(e=>(e=>{const{componentCls:t,iconCls:n,calc:o}=e;return{[t]:Object.assign(Object.assign({},(0,x.dF)(e)),{color:e.itemColor,fontSize:e.fontSize,[n]:{fontSize:e.iconFontSize},ol:{display:"flex",flexWrap:"wrap",margin:0,padding:0,listStyle:"none"},a:Object.assign({color:e.linkColor,transition:"color ".concat(e.motionDurationMid),padding:"0 ".concat((0,h.zA)(e.paddingXXS)),borderRadius:e.borderRadiusSM,height:e.fontHeight,display:"inline-block",marginInline:o(e.marginXXS).mul(-1).equal(),"&:hover":{color:e.linkHoverColor,backgroundColor:e.colorBgTextHover}},(0,x.K8)(e)),"li:last-child":{color:e.lastItemColor},["".concat(t,"-separator")]:{marginInline:e.separatorMargin,color:e.separatorColor},["".concat(t,"-link")]:{["\n          > ".concat(n," + span,\n          > ").concat(n," + a\n        ")]:{marginInlineStart:e.marginXXS}},["".concat(t,"-overlay-link")]:{borderRadius:e.borderRadiusSM,height:e.fontHeight,display:"inline-block",padding:"0 ".concat((0,h.zA)(e.paddingXXS)),marginInline:o(e.marginXXS).mul(-1).equal(),["> ".concat(n)]:{marginInlineStart:e.marginXXS,fontSize:e.fontSizeIcon},"&:hover":{color:e.linkHoverColor,backgroundColor:e.colorBgTextHover,a:{color:e.linkHoverColor}},a:{"&:hover":{backgroundColor:"transparent"}}},["&".concat(e.componentCls,"-rtl")]:{direction:"rtl"}})}})((0,k.h1)(e,{}))),(e=>({itemColor:e.colorTextDescription,lastItemColor:e.colorText,iconFontSize:e.fontSize,linkColor:e.colorTextDescription,linkHoverColor:e.colorText,separatorColor:e.colorTextDescription,separatorMargin:e.marginXS})));var S=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};function P(e){const{breadcrumbName:t,children:n}=e,o=S(e,["breadcrumbName","children"]),r=Object.assign({title:t},o);return n&&(r.menu={items:n.map((e=>{var{breadcrumbName:t}=e,n=S(e,["breadcrumbName"]);return Object.assign(Object.assign({},n),{title:t})}))}),r}var N=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};const w=e=>{const{prefixCls:t,separator:n="/",style:r,className:p,rootClassName:m,routes:u,items:f,children:b,itemRender:O,params:v={}}=e,C=N(e,["prefixCls","separator","style","className","rootClassName","routes","items","children","itemRender","params"]),{getPrefixCls:h,direction:x,breadcrumb:j}=o.useContext(s.QO);let k;const S=h("breadcrumb",t),[w,A,I]=E(S),T=function(e,t){return(0,o.useMemo)((()=>e||(t?t.map(P):null)),[e,t])}(f,u);const z=g(S,O);if(T&&T.length>0){const e=[],t=f||u;k=T.map(((r,l)=>{const{path:a,key:i,type:s,menu:p,overlay:m,onClick:u,className:f,separator:b,dropdownProps:g}=r,O=((e,t)=>{if(void 0===t)return t;let n=(t||"").replace(/^\//,"");return Object.keys(e).forEach((t=>{n=n.replace(":".concat(t),e[t])})),n})(v,a);void 0!==O&&e.push(O);const C=null!==i&&void 0!==i?i:l;if("separator"===s)return o.createElement(d,{key:C},b);const h={},x=l===T.length-1;p?h.menu=p:m&&(h.overlay=m);let{href:j}=r;return e.length&&void 0!==O&&(j="#/".concat(e.join("/"))),o.createElement(y,Object.assign({key:C},h,(0,c.A)(r,{data:!0,aria:!0}),{className:f,dropdownProps:g,href:j,separator:x?"":n,onClick:u,prefixCls:S}),z(r,v,t,e,j))}))}else if(b){const e=(0,a.A)(b).length;k=(0,a.A)(b).map(((t,o)=>{if(!t)return t;const r=o===e-1;return(0,i.Ob)(t,{separator:r?"":n,key:o})}))}const X=l()(S,null===j||void 0===j?void 0:j.className,{["".concat(S,"-rtl")]:"rtl"===x},p,m,A,I),R=Object.assign(Object.assign({},null===j||void 0===j?void 0:j.style),r);return w(o.createElement("nav",Object.assign({className:X,style:R},C),o.createElement("ol",null,k)))};w.Item=C,w.Separator=d;const A=w},59065:(e,t,n)=>{n.d(t,{A:()=>P});var o=n(76998),r=n(58736),l=n(34156),a=n.n(l),c=n(23551),i=n(36782),s=n(19727),p=n(96556),m=n(28037),u=n(20315),d=n(47917),f=n(8357),b=n(6932),g=n(50675),O=n(29457),y=n(41271),v=n(59395),C=n(55864);const h=(0,C.OF)("Popconfirm",(e=>(e=>{const{componentCls:t,iconCls:n,antCls:o,zIndexPopup:r,colorText:l,colorWarning:a,marginXXS:c,marginXS:i,fontSize:s,fontWeightStrong:p,colorTextHeading:m}=e;return{[t]:{zIndex:r,["&".concat(o,"-popover")]:{fontSize:s},["".concat(t,"-message")]:{marginBottom:i,display:"flex",flexWrap:"nowrap",alignItems:"start",["> ".concat(t,"-message-icon ").concat(n)]:{color:a,fontSize:s,lineHeight:1,marginInlineEnd:i},["".concat(t,"-title")]:{fontWeight:p,color:m,"&:only-child":{fontWeight:"normal"}},["".concat(t,"-description")]:{marginTop:c,color:l}},["".concat(t,"-buttons")]:{textAlign:"end",whiteSpace:"nowrap",button:{marginInlineStart:i}}}}})(e)),(e=>{const{zIndexPopupBase:t}=e;return{zIndexPopup:t+60}}),{resetStyle:!1});var x=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};const j=e=>{const{prefixCls:t,okButtonProps:n,cancelButtonProps:l,title:c,description:i,cancelText:s,okText:p,okType:u="primary",icon:v=o.createElement(r.A,null),showCancel:C=!0,close:h,onConfirm:x,onCancel:j,onPopupClick:k}=e,{getPrefixCls:E}=o.useContext(m.QO),[S]=(0,O.A)("Popconfirm",y.A.Popconfirm),P=(0,f.b)(c),N=(0,f.b)(i);return o.createElement("div",{className:"".concat(t,"-inner-content"),onClick:k},o.createElement("div",{className:"".concat(t,"-message")},v&&o.createElement("span",{className:"".concat(t,"-message-icon")},v),o.createElement("div",{className:"".concat(t,"-message-text")},P&&o.createElement("div",{className:a()("".concat(t,"-title"))},P),N&&o.createElement("div",{className:"".concat(t,"-description")},N))),o.createElement("div",{className:"".concat(t,"-buttons")},C&&o.createElement(b.Ay,Object.assign({onClick:j,size:"small"},l),s||(null===S||void 0===S?void 0:S.cancelText)),o.createElement(d.A,{buttonProps:Object.assign(Object.assign({size:"small"},(0,g.DU)(u)),n),actionFn:x,close:h,prefixCls:E("btn"),quitOnNullishReturnValue:!0,emitEvent:!0},p||(null===S||void 0===S?void 0:S.okText))))},k=e=>{const{prefixCls:t,placement:n,className:r,style:l}=e,c=x(e,["prefixCls","placement","className","style"]),{getPrefixCls:i}=o.useContext(m.QO),s=i("popconfirm",t),[p]=h(s);return p(o.createElement(v.Ay,{placement:n,className:a()(s,r),style:l,content:o.createElement(j,Object.assign({prefixCls:s},c))}))};var E=function(e,t){var n={};for(var o in e)Object.prototype.hasOwnProperty.call(e,o)&&t.indexOf(o)<0&&(n[o]=e[o]);if(null!=e&&"function"===typeof Object.getOwnPropertySymbols){var r=0;for(o=Object.getOwnPropertySymbols(e);r<o.length;r++)t.indexOf(o[r])<0&&Object.prototype.propertyIsEnumerable.call(e,o[r])&&(n[o[r]]=e[o[r]])}return n};const S=o.forwardRef(((e,t)=>{var n,l;const{prefixCls:d,placement:f="top",trigger:b="click",okType:g="primary",icon:O=o.createElement(r.A,null),children:y,overlayClassName:v,onOpenChange:C,onVisibleChange:x}=e,k=E(e,["prefixCls","placement","trigger","okType","icon","children","overlayClassName","onOpenChange","onVisibleChange"]),{getPrefixCls:S}=o.useContext(m.QO),[P,N]=(0,c.A)(!1,{value:null!==(n=e.open)&&void 0!==n?n:e.visible,defaultValue:null!==(l=e.defaultOpen)&&void 0!==l?l:e.defaultVisible}),w=(e,t)=>{N(e,!0),null===x||void 0===x||x(e),null===C||void 0===C||C(e,t)},A=S("popconfirm",d),I=a()(A,v),[T]=h(A);return T(o.createElement(u.A,Object.assign({},(0,s.A)(k,["title"]),{trigger:b,placement:f,onOpenChange:t=>{const{disabled:n=!1}=e;n||w(t)},open:P,ref:t,overlayClassName:I,content:o.createElement(j,Object.assign({okType:g,icon:O},e,{prefixCls:A,close:e=>{w(!1,e)},onConfirm:t=>{var n;return null===(n=e.onConfirm)||void 0===n?void 0:n.call(void 0,t)},onCancel:t=>{var n;w(!1,t),null===(n=e.onCancel)||void 0===n||n.call(void 0,t)}})),"data-popover-inject":!0}),(0,p.Ob)(y,{onKeyDown:e=>{var t,n;o.isValidElement(y)&&(null===(n=null===y||void 0===y?void 0:(t=y.props).onKeyDown)||void 0===n||n.call(t,e)),(e=>{e.keyCode===i.A.ESC&&P&&w(!1,e)})(e)}})))}));S._InternalPanelDoNotUseOrYouWillBeFired=k;const P=S}}]);
//# sourceMappingURL=498.0bf15884.chunk.js.map