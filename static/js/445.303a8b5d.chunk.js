"use strict";(self.webpackChunkbackend_ai_webui_react=self.webpackChunkbackend_ai_webui_react||[]).push([[445],{65113:function(n,e,c){c.d(e,{Z:function(){return I}});var t=c(52197),a=c(6851),i=c(10203),o=c(29439),r=c(4519),l=c(43270),d=c.n(l),s=c(49182),h=c(30516),g=c(48698),u=c(69381),p=c(53927),m=c(94382),f=function(n,e){var c={};for(var t in n)Object.prototype.hasOwnProperty.call(n,t)&&e.indexOf(t)<0&&(c[t]=n[t]);if(null!=n&&"function"===typeof Object.getOwnPropertySymbols){var a=0;for(t=Object.getOwnPropertySymbols(n);a<t.length;a++)e.indexOf(t[a])<0&&Object.prototype.propertyIsEnumerable.call(n,t[a])&&(c[t[a]]=n[t[a]])}return c},b=(0,h.i)((function(n){var e=n.prefixCls,c=n.className,t=n.closeIcon,a=n.closable,i=n.type,l=n.title,h=n.children,b=f(n,["prefixCls","className","closeIcon","closable","type","title","children"]),Z=r.useContext(g.E_).getPrefixCls,S=Z(),k=e||Z("modal"),I=(0,m.ZP)(k),w=(0,o.Z)(I,2)[1],v="".concat(k,"-confirm"),x={};return x=i?{closable:null!==a&&void 0!==a&&a,title:"",footer:"",children:r.createElement(u.O,Object.assign({},n,{prefixCls:k,confirmPrefixCls:v,rootPrefixCls:S,content:h}))}:{closable:null===a||void 0===a||a,title:l,footer:void 0===n.footer?r.createElement(p.$,Object.assign({},n)):n.footer,children:h},r.createElement(s.s,Object.assign({prefixCls:k,className:d()(w,"".concat(k,"-pure-panel"),i&&v,i&&"".concat(v,"-").concat(i),c)},b,{closeIcon:(0,p.b)(k,t),closable:a},x))})),Z=c(85591);function S(n){return(0,t.ZP)((0,t.uW)(n))}var k=i.Z;k.useModal=Z.Z,k.info=function(n){return(0,t.ZP)((0,t.cw)(n))},k.success=function(n){return(0,t.ZP)((0,t.vq)(n))},k.error=function(n){return(0,t.ZP)((0,t.AQ)(n))},k.warning=S,k.warn=S,k.confirm=function(n){return(0,t.ZP)((0,t.Au)(n))},k.destroyAll=function(){for(;a.Z.length;){var n=a.Z.pop();n&&n()}},k.config=t.ai,k._InternalPanelDoNotUseOrYouWillBeFired=b;var I=k},62708:function(n,e,c){c.d(e,{Z:function(){return j}});var t=c(4942),a=c(29439),i=c(4519),o=c(32064),r=c(43270),l=c.n(r),d=c(87462),s=c(44925),h=c(269),g=c(33714),u=["prefixCls","className","checked","defaultChecked","disabled","loadingIcon","checkedChildren","unCheckedChildren","onClick","onChange","onKeyDown"],p=i.forwardRef((function(n,e){var c,o=n.prefixCls,r=void 0===o?"rc-switch":o,p=n.className,m=n.checked,f=n.defaultChecked,b=n.disabled,Z=n.loadingIcon,S=n.checkedChildren,k=n.unCheckedChildren,I=n.onClick,w=n.onChange,v=n.onKeyDown,x=(0,s.Z)(n,u),C=(0,h.Z)(!1,{value:m,defaultValue:f}),y=(0,a.Z)(C,2),E=y[0],M=y[1];function O(n,e){var c=E;return b||(M(c=n),null===w||void 0===w||w(c,e)),c}var P=l()(r,p,(c={},(0,t.Z)(c,"".concat(r,"-checked"),E),(0,t.Z)(c,"".concat(r,"-disabled"),b),c));return i.createElement("button",(0,d.Z)({},x,{type:"button",role:"switch","aria-checked":E,disabled:b,className:P,ref:e,onKeyDown:function(n){n.which===g.Z.LEFT?O(!1,n):n.which===g.Z.RIGHT&&O(!0,n),null===v||void 0===v||v(n)},onClick:function(n){var e=O(!E,n);null===I||void 0===I||I(e,n)}}),Z,i.createElement("span",{className:"".concat(r,"-inner")},i.createElement("span",{className:"".concat(r,"-inner-checked")},S),i.createElement("span",{className:"".concat(r,"-inner-unchecked")},k)))}));p.displayName="Switch";var m=p,f=c(99495),b=c(48698),Z=c(46963),S=c(76569),k=c(60989),I=c(21480),w=c(70111),v=c(41157),x=function(n){var e,c,a,i,o,r=n.componentCls,l=n.trackHeightSM,d=n.trackPadding,s=n.trackMinWidthSM,h=n.innerMinMarginSM,g=n.innerMaxMarginSM,u=n.handleSizeSM,p="".concat(r,"-inner");return(0,t.Z)({},r,(0,t.Z)({},"&".concat(r,"-small"),(o={minWidth:s,height:l,lineHeight:"".concat(l,"px")},(0,t.Z)(o,"".concat(r,"-inner"),(e={paddingInlineStart:g,paddingInlineEnd:h},(0,t.Z)(e,"".concat(p,"-checked"),{marginInlineStart:"calc(-100% + ".concat(u+2*d,"px - ").concat(2*g,"px)"),marginInlineEnd:"calc(100% - ".concat(u+2*d,"px + ").concat(2*g,"px)")}),(0,t.Z)(e,"".concat(p,"-unchecked"),{marginTop:-l,marginInlineStart:0,marginInlineEnd:0}),e)),(0,t.Z)(o,"".concat(r,"-handle"),{width:u,height:u}),(0,t.Z)(o,"".concat(r,"-loading-icon"),{top:(u-n.switchLoadingIconSize)/2,fontSize:n.switchLoadingIconSize}),(0,t.Z)(o,"&".concat(r,"-checked"),(a={},(0,t.Z)(a,"".concat(r,"-inner"),(c={paddingInlineStart:h,paddingInlineEnd:g},(0,t.Z)(c,"".concat(p,"-checked"),{marginInlineStart:0,marginInlineEnd:0}),(0,t.Z)(c,"".concat(p,"-unchecked"),{marginInlineStart:"calc(100% - ".concat(u+2*d,"px + ").concat(2*g,"px)"),marginInlineEnd:"calc(-100% + ".concat(u+2*d,"px - ").concat(2*g,"px)")}),c)),(0,t.Z)(a,"".concat(r,"-handle"),{insetInlineStart:"calc(100% - ".concat(u+d,"px)")}),a)),(0,t.Z)(o,"&:not(".concat(r,"-disabled):active"),(i={},(0,t.Z)(i,"&:not(".concat(r,"-checked) ").concat(p),(0,t.Z)({},"".concat(p,"-unchecked"),{marginInlineStart:n.marginXXS/2,marginInlineEnd:-n.marginXXS/2})),(0,t.Z)(i,"&".concat(r,"-checked ").concat(p),(0,t.Z)({},"".concat(p,"-checked"),{marginInlineStart:-n.marginXXS/2,marginInlineEnd:n.marginXXS/2})),i)),o)))},C=function(n){var e,c=n.componentCls,a=n.handleSize;return(0,t.Z)({},c,(e={},(0,t.Z)(e,"".concat(c,"-loading-icon").concat(n.iconCls),{position:"relative",top:(a-n.fontSize)/2,color:n.switchLoadingIconColor,verticalAlign:"top"}),(0,t.Z)(e,"&".concat(c,"-checked ").concat(c,"-loading-icon"),{color:n.switchColor}),e))},y=function(n){var e,c,a=n.componentCls,i=n.motion,o=n.trackPadding,r=n.handleBg,l=n.handleShadow,d=n.handleSize,s="".concat(a,"-handle");return(0,t.Z)({},a,(c={},(0,t.Z)(c,s,{position:"absolute",top:o,insetInlineStart:o,width:d,height:d,transition:"all ".concat(n.switchDuration," ease-in-out"),"&::before":{position:"absolute",top:0,insetInlineEnd:0,bottom:0,insetInlineStart:0,backgroundColor:r,borderRadius:d/2,boxShadow:l,transition:"all ".concat(n.switchDuration," ease-in-out"),content:'""'}}),(0,t.Z)(c,"&".concat(a,"-checked ").concat(s),{insetInlineStart:"calc(100% - ".concat(d+o,"px)")}),(0,t.Z)(c,"&:not(".concat(a,"-disabled):active"),i?(e={},(0,t.Z)(e,"".concat(s,"::before"),{insetInlineEnd:n.switchHandleActiveInset,insetInlineStart:0}),(0,t.Z)(e,"&".concat(a,"-checked ").concat(s,"::before"),{insetInlineEnd:0,insetInlineStart:n.switchHandleActiveInset}),e):{}),c))},E=function(n){var e,c,a,i,o=n.componentCls,r=n.trackHeight,l=n.trackPadding,d=n.innerMinMargin,s=n.innerMaxMargin,h=n.handleSize,g="".concat(o,"-inner");return(0,t.Z)({},o,(i={},(0,t.Z)(i,g,(e={display:"block",overflow:"hidden",borderRadius:100,height:"100%",paddingInlineStart:s,paddingInlineEnd:d,transition:"padding-inline-start ".concat(n.switchDuration," ease-in-out, padding-inline-end ").concat(n.switchDuration," ease-in-out")},(0,t.Z)(e,"".concat(g,"-checked, ").concat(g,"-unchecked"),{display:"block",color:n.colorTextLightSolid,fontSize:n.fontSizeSM,transition:"margin-inline-start ".concat(n.switchDuration," ease-in-out, margin-inline-end ").concat(n.switchDuration," ease-in-out"),pointerEvents:"none"}),(0,t.Z)(e,"".concat(g,"-checked"),{marginInlineStart:"calc(-100% + ".concat(h+2*l,"px - ").concat(2*s,"px)"),marginInlineEnd:"calc(100% - ".concat(h+2*l,"px + ").concat(2*s,"px)")}),(0,t.Z)(e,"".concat(g,"-unchecked"),{marginTop:-r,marginInlineStart:0,marginInlineEnd:0}),e)),(0,t.Z)(i,"&".concat(o,"-checked ").concat(g),(c={paddingInlineStart:d,paddingInlineEnd:s},(0,t.Z)(c,"".concat(g,"-checked"),{marginInlineStart:0,marginInlineEnd:0}),(0,t.Z)(c,"".concat(g,"-unchecked"),{marginInlineStart:"calc(100% - ".concat(h+2*l,"px + ").concat(2*s,"px)"),marginInlineEnd:"calc(-100% + ".concat(h+2*l,"px - ").concat(2*s,"px)")}),c)),(0,t.Z)(i,"&:not(".concat(o,"-disabled):active"),(a={},(0,t.Z)(a,"&:not(".concat(o,"-checked) ").concat(g),(0,t.Z)({},"".concat(g,"-unchecked"),{marginInlineStart:2*l,marginInlineEnd:2*-l})),(0,t.Z)(a,"&".concat(o,"-checked ").concat(g),(0,t.Z)({},"".concat(g,"-checked"),{marginInlineStart:2*-l,marginInlineEnd:2*l})),a)),i))},M=function(n){var e,c=n.componentCls,a=n.trackHeight,i=n.trackMinWidth;return(0,t.Z)({},c,Object.assign(Object.assign(Object.assign(Object.assign({},(0,I.Wf)(n)),(0,t.Z)({position:"relative",display:"inline-block",boxSizing:"border-box",minWidth:i,height:a,lineHeight:"".concat(a,"px"),verticalAlign:"middle",background:n.colorTextQuaternary,border:"0",borderRadius:100,cursor:"pointer",transition:"all ".concat(n.motionDurationMid),userSelect:"none"},"&:hover:not(".concat(c,"-disabled)"),{background:n.colorTextTertiary})),(0,I.Qy)(n)),(e={},(0,t.Z)(e,"&".concat(c,"-checked"),(0,t.Z)({background:n.switchColor},"&:hover:not(".concat(c,"-disabled)"),{background:n.colorPrimaryHover})),(0,t.Z)(e,"&".concat(c,"-loading, &").concat(c,"-disabled"),{cursor:"not-allowed",opacity:n.switchDisabledOpacity,"*":{boxShadow:"none",cursor:"not-allowed"}}),(0,t.Z)(e,"&".concat(c,"-rtl"),{direction:"rtl"}),e)))},O=(0,w.Z)("Switch",(function(n){var e=(0,v.TS)(n,{switchDuration:n.motionDurationMid,switchColor:n.colorPrimary,switchDisabledOpacity:n.opacityLoading,switchLoadingIconSize:.75*n.fontSizeIcon,switchLoadingIconColor:"rgba(0, 0, 0, ".concat(n.opacityLoading,")"),switchHandleActiveInset:"-30%"});return[M(e),E(e),y(e),C(e),x(e)]}),(function(n){var e=n.fontSize*n.lineHeight,c=n.controlHeight/2,t=e-4,a=c-4;return{trackHeight:e,trackHeightSM:c,trackMinWidth:2*t+8,trackMinWidthSM:2*a+4,trackPadding:2,handleBg:n.colorWhite,handleSize:t,handleSizeSM:a,handleShadow:"0 2px 4px 0 ".concat(new k.C("#00230b").setAlpha(.2).toRgbString()),innerMinMargin:t/2,innerMaxMargin:t+2+4,innerMinMarginSM:a/2,innerMaxMarginSM:a+2+4}})),P=function(n,e){var c={};for(var t in n)Object.prototype.hasOwnProperty.call(n,t)&&e.indexOf(t)<0&&(c[t]=n[t]);if(null!=n&&"function"===typeof Object.getOwnPropertySymbols){var a=0;for(t=Object.getOwnPropertySymbols(n);a<t.length;a++)e.indexOf(t[a])<0&&Object.prototype.propertyIsEnumerable.call(n,t[a])&&(c[t[a]]=n[t[a]])}return c},N=i.forwardRef((function(n,e){var c,r=n.prefixCls,d=n.size,s=n.disabled,h=n.loading,g=n.className,u=n.rootClassName,p=n.style,k=P(n,["prefixCls","size","disabled","loading","className","rootClassName","style"]),I=i.useContext(b.E_),w=I.getPrefixCls,v=I.direction,x=I.switch,C=i.useContext(Z.Z),y=(null!==s&&void 0!==s?s:C)||h,E=w("switch",r),M=i.createElement("div",{className:"".concat(E,"-handle")},h&&i.createElement(o.Z,{className:"".concat(E,"-loading-icon")})),N=O(E),j=(0,a.Z)(N,2),z=j[0],D=j[1],H=(0,S.Z)(d),_=l()(null===x||void 0===x?void 0:x.className,(c={},(0,t.Z)(c,"".concat(E,"-small"),"small"===H),(0,t.Z)(c,"".concat(E,"-loading"),h),(0,t.Z)(c,"".concat(E,"-rtl"),"rtl"===v),c),g,u,D),T=Object.assign(Object.assign({},null===x||void 0===x?void 0:x.style),p);return z(i.createElement(f.Z,{component:"Switch"},i.createElement(m,Object.assign({},k,{prefixCls:E,className:_,style:T,disabled:y,ref:e,loadingIcon:M}))))}));N.__ANT_SWITCH=!0;var j=N}}]);
//# sourceMappingURL=445.303a8b5d.chunk.js.map