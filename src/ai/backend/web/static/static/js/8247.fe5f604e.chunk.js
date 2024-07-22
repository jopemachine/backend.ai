"use strict";(self.webpackChunkbackend_ai_webui_react=self.webpackChunkbackend_ai_webui_react||[]).push([[8247],{68247:(e,n,a)=>{a.r(n),a.d(n,{default:()=>O});var r,i,t,l=a(30032),s=a(83468),o=a(21454),d=a(9016),u=a(34103),c=a(24717),g=a(90214),m=a(7121),y=a(63284),p=a(87627),f=a(78074),h=a(45901),k=a.n(h),v=a(76998),b=a(77678),C=a(3606),x=a(23446);const R=e=>{var n;let{existingHostnames:l,containerRegistryFrgmt:s=null,onOk:o,...d}=e;const{t:h}=(0,b.Bd)(),R=(0,v.useRef)(null),[F,A]=c.Ay.useMessage(),[j,w]=g.A.useModal(),S=(0,C.useFragment)(void 0!==r?r:r=a(8869),s),[_,K]=(0,C.useMutation)(void 0!==i?i:i=a(47858)),[M,L]=(0,C.useMutation)(void 0!==t?t:t=a(7772)),T=async()=>{var e;return null===(e=R.current)||void 0===e?void 0:e.validateFields().then((e=>{const n={hostname:e.hostname,props:{url:e.config.url,type:e.config.type,project:"docker"===e.config.project?void 0:e.config.project,username:k().isEmpty(e.config.username)?null:e.config.username,password:e.isChangedPassword?k().isEmpty(e.config.password)?null:e.config.password:void 0}};S?(e.isChangedPassword||delete n.props.password,M({variables:n,onCompleted:(e,n)=>{n?F.error(h("dialog.ErrorOccurred")):o&&o("modify")},onError:e=>{F.error(h("dialog.ErrorOccurred"))}})):_({variables:n,onCompleted:(e,n)=>{n?F.error(h("dialog.ErrorOccurred")):o&&o("create")},onError(e){F.error(h("dialog.ErrorOccurred"))}})})).catch((e=>{}))};return(0,x.jsxs)(u.A,{title:h(S?"registry.ModifyRegistry":"registry.AddRegistry"),okText:h(S?"button.Save":"button.Add"),confirmLoading:K||L,onOk:()=>{var e;null===(e=R.current)||void 0===e||e.validateFields().then((e=>{var n;k().includes(null===(n=e.config)||void 0===n?void 0:n.type,"harbor")&&(k().isEmpty(e.config.username)||(S?e.isChangedPassword&&k().isEmpty(e.config.password):k().isEmpty(e.config.password)))?j.confirm({title:h("button.Confirm"),content:h("registry.ConfirmNoUserName"),onOk:()=>{T()}}):T()})).catch((()=>{}))},...d,destroyOnClose:!0,children:[A,w,(0,x.jsxs)(m.A,{ref:R,layout:"vertical",requiredMark:"optional",initialValues:S?{...S,config:{...S.config,project:(null===(n=S.config)||void 0===n?void 0:n.project)||void 0}}:{config:{type:"docker"}},preserve:!1,children:[(0,x.jsx)(m.A.Item,{label:h("registry.Hostname"),name:"hostname",required:!0,rules:[{required:!0,message:h("registry.DescHostnameIsEmpty"),pattern:new RegExp("^.+$")},{validator:(e,n)=>!S&&null!==l&&void 0!==l&&l.includes(n)?Promise.reject(h("registry.RegistryHostnameAlreadyExists")):Promise.resolve()}],children:(0,x.jsx)(y.A,{disabled:!!S,value:(null===S||void 0===S?void 0:S.hostname)||void 0})}),(0,x.jsx)(m.A.Item,{name:["config","url"],label:h("registry.RegistryURL"),required:!0,rules:[{required:!0},{validator:(e,n)=>{if(n){if(!n.startsWith("http://")&&!n.startsWith("https://"))return Promise.reject(h("registry.DescURLStartString"));try{new URL(n)}catch(a){return Promise.reject(h("registry.DescURLFormat"))}}return Promise.resolve()}}],children:(0,x.jsx)(y.A,{})}),(0,x.jsx)(m.A.Item,{noStyle:!0,shouldUpdate:(e,n)=>{var a,r;return k().isEmpty(null===(a=e.config)||void 0===a?void 0:a.password)!==k().isEmpty(null===(r=n.config)||void 0===r?void 0:r.password)},children:e=>{let{validateFields:n,getFieldValue:a}=e;return n([["config","username"]]),(0,x.jsx)(m.A.Item,{name:["config","username"],label:h("registry.Username"),rules:[{required:!k().isEmpty(a(["config","password"]))}],children:(0,x.jsx)(y.A,{})})}}),(0,x.jsxs)(m.A.Item,{label:h("registry.Password"),children:[(0,x.jsx)(m.A.Item,{noStyle:!0,shouldUpdate:(e,n)=>e.isChangedPassword!==n.isChangedPassword,children:e=>{let{getFieldValue:n}=e;return(0,x.jsx)(m.A.Item,{noStyle:!0,name:["config","password"],children:(0,x.jsx)(y.A.Password,{disabled:!k().isEmpty(S)&&!n("isChangedPassword")})})}}),!k().isEmpty(S)&&(0,x.jsx)(m.A.Item,{noStyle:!0,name:"isChangedPassword",valuePropName:"checked",children:(0,x.jsx)(p.A,{onChange:e=>{var n;e.target.checked||(null===(n=R.current)||void 0===n||n.setFieldValue(["config","password"],""))},children:h("webui.menu.ChangePassword")})})]}),(0,x.jsx)(m.A.Item,{name:["config","type"],label:h("registry.RegistryType"),required:!0,rules:[{required:!0,message:h("registry.PleaseSelectOption")}],children:(0,x.jsx)(f.A,{options:[{value:"docker"},{value:"harbor"},{value:"harbor2"}],onChange:()=>{}})}),(0,x.jsx)(m.A.Item,{shouldUpdate:(e,n)=>{var a,r;return(null===e||void 0===e||null===(a=e.config)||void 0===a?void 0:a.type)!==(null===n||void 0===n||null===(r=n.config)||void 0===r?void 0:r.type)},noStyle:!0,children:e=>{let{getFieldValue:n}=e;return"docker"!==n(["config","type"])&&(0,x.jsx)(m.A.Item,{name:["config","project"],label:h("registry.ProjectName"),required:!0,rules:[{required:!0,message:h("registry.ProjectNameIsRequired")}],children:(0,x.jsx)(f.A,{mode:"tags",open:!1,tokenSeparators:[","," "],suffixIcon:null})})}})]})]})};var F,A,j,w=a(29871),S=a(94519),_=a(71038),K=a(63718),M=a(97041),L=a(69048),T=a(22424),E=a(50840),I=a(20307),D=a(6932),q=a(94120),N=a(48713),P=a(82745),V=a(97080);const O=e=>{let{style:n}=e;const r=(0,s.CX)(),[i,t]=(0,s.Tw)("initial-fetch"),[g,p]=(0,v.useTransition)(),f=(0,d.b)(),[h,O]=c.Ay.useMessage(),{upsertNotification:U}=(0,o.js)(),{container_registries:$,domain:B}=(0,C.useLazyLoadQuery)(void 0!==F?F:F=a(27067),{domain:r._config.domainName},{fetchPolicy:"store-and-network",fetchKey:i}),[H,z]=(0,C.useMutation)(void 0!==A?A:A=a(45341)),[Q,Y]=(0,C.useMutation)(void 0!==j?j:j=a(80754)),{t:W}=(0,b.Bd)(),{token:G}=E.A.useToken(),[X,J]=(0,v.useState)(null),[Z,ee]=(0,v.useState)(null),[ne,ae]=(0,v.useState)(""),[re,ie]=(0,v.useState)(!1),[te,le]=(0,v.useState)();return(0,x.jsxs)(w.A,{direction:"column",align:"stretch",style:{flex:1,...n},children:[O,(0,x.jsxs)(w.A,{direction:"row",justify:"end",gap:"sm",style:{padding:G.paddingSM},children:[(0,x.jsx)(I.A,{title:W("button.Refresh"),children:(0,x.jsx)(D.Ay,{loading:g,icon:(0,x.jsx)(S.A,{}),onClick:()=>{p((()=>{t()}))}})}),(0,x.jsx)(D.Ay,{type:"primary",icon:(0,x.jsx)(_.A,{}),onClick:()=>{ie(!0)},children:W("registry.AddRegistry")})]}),(0,x.jsx)(q.A,{rowKey:e=>e.id,scroll:{x:"max-content"},pagination:!1,columns:[{title:W("registry.Hostname"),dataIndex:"hostname"},{title:W("registry.RegistryURL"),dataIndex:["config","url"]},{title:W("registry.Type"),dataIndex:["config","type"]},{title:W("registry.HarborProject"),render:(e,n)=>{var a;return k().map(null===(a=n.config)||void 0===a?void 0:a.project,(e=>(0,x.jsx)(N.A,{children:e||""},e)))}},{title:W("registry.Username"),dataIndex:["config","username"]},{title:W("registry.Password"),dataIndex:["config","password"]},{title:W("general.Enabled"),render:(e,n)=>{const a=k().includes(null===B||void 0===B?void 0:B.allowed_docker_registries,n.hostname);return(0,x.jsx)(P.A,{checked:te===n.hostname+i?!a:a,disabled:g||Y,loading:te===n.hostname+i,onChange:e=>{if(!k().isString(n.hostname))return;let a=k().clone((null===B||void 0===B?void 0:B.allowed_docker_registries)||[]);e?a.push(n.hostname):a=k().without(a,n.hostname),le(n.hostname+i),Q({variables:{domain:r._config.domainName,allowed_docker_registries:a},onCompleted:n=>{p((()=>{t()})),h.info({key:"registry-enabled",content:W(e?"registry.RegistryTurnedOn":"registry.RegistryTurnedOff")})}})}})}},{title:W("general.Control"),fixed:"right",render:(e,n,a)=>(0,x.jsxs)(w.A,{children:[(0,x.jsx)(I.A,{title:W("button.Edit"),children:(0,x.jsx)(D.Ay,{size:"large",style:{color:G.colorInfo},type:"text",icon:(0,x.jsx)(K.A,{}),onClick:()=>{J(n)}})}),(0,x.jsx)(I.A,{title:W("button.Delete"),children:(0,x.jsx)(D.Ay,{size:"large",danger:!0,type:"text",icon:(0,x.jsx)(M.A,{}),onClick:()=>{ee(n)}})}),(0,x.jsx)(I.A,{title:W("maintenance.RescanImages"),children:(0,x.jsx)(D.Ay,{size:"large",type:"text",icon:(0,x.jsx)(L.A,{onClick:()=>{n.hostname&&(async e=>{const n=U({message:"".concat(e," ").concat(W("maintenance.RescanImages")),description:W("registry.UpdatingRegistryInfo"),open:!0,backgroundTask:{status:"pending"},duration:0});r.maintenance.rescan_images(e).then((e=>{let{rescan_images:a}=e;a.ok?U({key:n,backgroundTask:{status:"pending",percent:0,taskId:a.task_id,statusDescriptions:{pending:W("registry.RescanImages"),resolved:W("registry.RegistryUpdateFinished"),rejected:W("registry.RegistryUpdateFailed")}}}):U({key:n,backgroundTask:{status:"rejected"},duration:1})})).catch((e=>{console.log(e),U({key:n,backgroundTask:{status:"rejected"},duration:1}),e&&e.message&&(globalThis.lablupNotification.text=f.relieve(e.title),globalThis.lablupNotification.detail=e.message,globalThis.lablupNotification.show(!0,e))}))})(n.hostname)}})})})]})}],dataSource:(0,l.tS)($)}),(0,x.jsx)(R,{containerRegistryFrgmt:X,existingHostnames:k().map($,(e=>(null===e||void 0===e?void 0:e.hostname)||"")),open:!!X||re,onOk:e=>{"create"===e?(t(),h.info(W("registry.RegistrySuccessfullyAdded"))):"modify"===e&&h.info(W("registry.RegistrySuccessfullyModified")),J(null),ie(!1)},onCancel:()=>{J(null),ie(!1)},centered:!1}),(0,x.jsx)(u.A,{title:(0,x.jsxs)(x.Fragment,{children:[(0,x.jsx)(T.A,{style:{color:G.colorWarning}})," ",W("dialog.warning.CannotBeUndone")]}),okText:W("button.Delete"),okButtonProps:{danger:!0,disabled:ne!==(null===Z||void 0===Z?void 0:Z.hostname)},onOk:()=>{Z?H({variables:{hostname:Z.hostname||""},onCompleted:(e,n)=>{n?(ee(null),h.error({key:"registry-deletion-failed",content:W("dialog.ErrorOccurred")})):(p((()=>{t()})),h.info({key:"registry-deleted",content:W("registry.RegistrySuccessfullyDeleted")}),ee(null))},onError:e=>{h.error({key:"registry-deletion-failed",content:W("dialog.ErrorOccurred")})}}):ee(null)},confirmLoading:z,onCancel:()=>{ee(null)},destroyOnClose:!0,open:!!Z,children:(0,x.jsxs)(w.A,{direction:"column",align:"stretch",gap:"sm",style:{marginTop:G.marginMD},children:[(0,x.jsxs)(V.A.Text,{children:[(0,x.jsx)(V.A.Text,{code:!0,children:null===Z||void 0===Z?void 0:Z.hostname})," ",W("registry.TypeRegistryNameToDelete")]}),(0,x.jsx)(m.A,{children:(0,x.jsx)(m.A.Item,{name:"confirmText",rules:[{required:!0,message:W("registry.HostnameDoesNotMatch"),validator:async()=>ne===(null===Z||void 0===Z?void 0:Z.hostname)?Promise.resolve():Promise.reject()}],children:(0,x.jsx)(y.A,{autoComplete:"off",value:ne,onChange:e=>ae(e.target.value)})})})]})})]})}},47858:(e,n,a)=>{a.r(n),a.d(n,{default:()=>i});const r=function(){var e=[{defaultValue:null,kind:"LocalArgument",name:"hostname"},{defaultValue:null,kind:"LocalArgument",name:"props"}],n=[{alias:null,args:[{kind:"Variable",name:"hostname",variableName:"hostname"},{kind:"Variable",name:"props",variableName:"props"}],concreteType:"CreateContainerRegistry",kind:"LinkedField",name:"create_container_registry",plural:!1,selections:[{alias:null,args:null,concreteType:"ContainerRegistry",kind:"LinkedField",name:"container_registry",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"id",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"hostname",storageKey:null},{alias:null,args:null,concreteType:"ContainerRegistryConfig",kind:"LinkedField",name:"config",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"url",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"type",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"project",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"username",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"password",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"ssl_verify",storageKey:null}],storageKey:null}],storageKey:null}],storageKey:null}];return{fragment:{argumentDefinitions:e,kind:"Fragment",metadata:null,name:"ContainerRegistryEditorModalCreateMutation",selections:n,type:"Mutations",abstractKey:null},kind:"Request",operation:{argumentDefinitions:e,kind:"Operation",name:"ContainerRegistryEditorModalCreateMutation",selections:n},params:{cacheID:"f076fed995ef5122c04aaba2b76d22d5",id:null,metadata:{},name:"ContainerRegistryEditorModalCreateMutation",operationKind:"mutation",text:"mutation ContainerRegistryEditorModalCreateMutation(\n  $hostname: String!\n  $props: CreateContainerRegistryInput!\n) {\n  create_container_registry(hostname: $hostname, props: $props) {\n    container_registry {\n      id\n      hostname\n      config {\n        url\n        type\n        project\n        username\n        password\n        ssl_verify\n      }\n    }\n  }\n}\n"}}}();r.hash="547863dc03f9faf465e8a26175e5a101";const i=r},8869:(e,n,a)=>{a.r(n),a.d(n,{default:()=>i});const r={argumentDefinitions:[],kind:"Fragment",metadata:null,name:"ContainerRegistryEditorModalFragment",selections:[{alias:null,args:null,kind:"ScalarField",name:"id",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"hostname",storageKey:null},{alias:null,args:null,concreteType:"ContainerRegistryConfig",kind:"LinkedField",name:"config",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"url",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"type",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"project",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"username",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"ssl_verify",storageKey:null}],storageKey:null}],type:"ContainerRegistry",abstractKey:null,hash:"57f748e00ec698f8b3e133570e377c71"},i=r},7772:(e,n,a)=>{a.r(n),a.d(n,{default:()=>i});const r=function(){var e=[{defaultValue:null,kind:"LocalArgument",name:"hostname"},{defaultValue:null,kind:"LocalArgument",name:"props"}],n=[{alias:null,args:[{kind:"Variable",name:"hostname",variableName:"hostname"},{kind:"Variable",name:"props",variableName:"props"}],concreteType:"ModifyContainerRegistry",kind:"LinkedField",name:"modify_container_registry",plural:!1,selections:[{alias:null,args:null,concreteType:"ContainerRegistry",kind:"LinkedField",name:"container_registry",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"id",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"hostname",storageKey:null},{alias:null,args:null,concreteType:"ContainerRegistryConfig",kind:"LinkedField",name:"config",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"url",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"type",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"project",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"username",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"password",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"ssl_verify",storageKey:null}],storageKey:null}],storageKey:null}],storageKey:null}];return{fragment:{argumentDefinitions:e,kind:"Fragment",metadata:null,name:"ContainerRegistryEditorModalModifyMutation",selections:n,type:"Mutations",abstractKey:null},kind:"Request",operation:{argumentDefinitions:e,kind:"Operation",name:"ContainerRegistryEditorModalModifyMutation",selections:n},params:{cacheID:"5157cbd573b855028a9856c295dab17c",id:null,metadata:{},name:"ContainerRegistryEditorModalModifyMutation",operationKind:"mutation",text:"mutation ContainerRegistryEditorModalModifyMutation(\n  $hostname: String!\n  $props: ModifyContainerRegistryInput!\n) {\n  modify_container_registry(hostname: $hostname, props: $props) {\n    container_registry {\n      id\n      hostname\n      config {\n        url\n        type\n        project\n        username\n        password\n        ssl_verify\n      }\n    }\n  }\n}\n"}}}();r.hash="0fe59a8682e5d7c502bc48da7cc3320a";const i=r},45341:(e,n,a)=>{a.r(n),a.d(n,{default:()=>i});const r=function(){var e=[{defaultValue:null,kind:"LocalArgument",name:"hostname"}],n=[{alias:null,args:[{kind:"Variable",name:"hostname",variableName:"hostname"}],concreteType:"DeleteContainerRegistry",kind:"LinkedField",name:"delete_container_registry",plural:!1,selections:[{alias:null,args:null,concreteType:"ContainerRegistry",kind:"LinkedField",name:"container_registry",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"id",storageKey:null}],storageKey:null}],storageKey:null}];return{fragment:{argumentDefinitions:e,kind:"Fragment",metadata:null,name:"ContainerRegistryListDeleteMutation",selections:n,type:"Mutations",abstractKey:null},kind:"Request",operation:{argumentDefinitions:e,kind:"Operation",name:"ContainerRegistryListDeleteMutation",selections:n},params:{cacheID:"4482108b7e256e27225f91b432f0a856",id:null,metadata:{},name:"ContainerRegistryListDeleteMutation",operationKind:"mutation",text:"mutation ContainerRegistryListDeleteMutation(\n  $hostname: String!\n) {\n  delete_container_registry(hostname: $hostname) {\n    container_registry {\n      id\n    }\n  }\n}\n"}}}();r.hash="131801c7f9561f645743bbbe4b464676";const i=r},80754:(e,n,a)=>{a.r(n),a.d(n,{default:()=>i});const r=function(){var e={defaultValue:null,kind:"LocalArgument",name:"allowed_docker_registries"},n={defaultValue:null,kind:"LocalArgument",name:"domain"},a=[{alias:null,args:[{kind:"Variable",name:"name",variableName:"domain"},{fields:[{kind:"Variable",name:"allowed_docker_registries",variableName:"allowed_docker_registries"}],kind:"ObjectValue",name:"props"}],concreteType:"ModifyDomain",kind:"LinkedField",name:"modify_domain",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"ok",storageKey:null}],storageKey:null}];return{fragment:{argumentDefinitions:[e,n],kind:"Fragment",metadata:null,name:"ContainerRegistryListDomainMutation",selections:a,type:"Mutations",abstractKey:null},kind:"Request",operation:{argumentDefinitions:[n,e],kind:"Operation",name:"ContainerRegistryListDomainMutation",selections:a},params:{cacheID:"569e278934331792cc007f79e8e4c44c",id:null,metadata:{},name:"ContainerRegistryListDomainMutation",operationKind:"mutation",text:"mutation ContainerRegistryListDomainMutation(\n  $domain: String!\n  $allowed_docker_registries: [String]!\n) {\n  modify_domain(name: $domain, props: {allowed_docker_registries: $allowed_docker_registries}) {\n    ok\n  }\n}\n"}}}();r.hash="d8730f4ae61e895ec64fb03e53998b97";const i=r},27067:(e,n,a)=>{a.r(n),a.d(n,{default:()=>i});const r=function(){var e=[{defaultValue:null,kind:"LocalArgument",name:"domain"}],n={alias:null,args:null,kind:"ScalarField",name:"id",storageKey:null},a={alias:null,args:null,kind:"ScalarField",name:"hostname",storageKey:null},r={alias:null,args:null,kind:"ScalarField",name:"url",storageKey:null},i={alias:null,args:null,kind:"ScalarField",name:"type",storageKey:null},t={alias:null,args:null,kind:"ScalarField",name:"project",storageKey:null},l={alias:null,args:null,kind:"ScalarField",name:"username",storageKey:null},s={alias:null,args:null,kind:"ScalarField",name:"password",storageKey:null},o={alias:null,args:null,kind:"ScalarField",name:"ssl_verify",storageKey:null},d={alias:null,args:[{kind:"Variable",name:"name",variableName:"domain"}],concreteType:"Domain",kind:"LinkedField",name:"domain",plural:!1,selections:[{alias:null,args:null,kind:"ScalarField",name:"name",storageKey:null},{alias:null,args:null,kind:"ScalarField",name:"allowed_docker_registries",storageKey:null}],storageKey:null};return{fragment:{argumentDefinitions:e,kind:"Fragment",metadata:null,name:"ContainerRegistryListQuery",selections:[{alias:null,args:null,concreteType:"ContainerRegistry",kind:"LinkedField",name:"container_registries",plural:!0,selections:[{args:null,kind:"FragmentSpread",name:"ContainerRegistryEditorModalFragment"},n,a,{alias:null,args:null,concreteType:"ContainerRegistryConfig",kind:"LinkedField",name:"config",plural:!1,selections:[r,i,t,l,s,o],storageKey:null}],storageKey:null},d],type:"Queries",abstractKey:null},kind:"Request",operation:{argumentDefinitions:e,kind:"Operation",name:"ContainerRegistryListQuery",selections:[{alias:null,args:null,concreteType:"ContainerRegistry",kind:"LinkedField",name:"container_registries",plural:!0,selections:[n,a,{alias:null,args:null,concreteType:"ContainerRegistryConfig",kind:"LinkedField",name:"config",plural:!1,selections:[r,i,t,l,o,s],storageKey:null}],storageKey:null},d]},params:{cacheID:"9f9e16fde586191ea3e0ae553f834b51",id:null,metadata:{},name:"ContainerRegistryListQuery",operationKind:"query",text:"query ContainerRegistryListQuery(\n  $domain: String!\n) {\n  container_registries {\n    ...ContainerRegistryEditorModalFragment\n    id\n    hostname\n    config {\n      url\n      type\n      project\n      username\n      password\n      ssl_verify\n    }\n  }\n  domain(name: $domain) {\n    name\n    allowed_docker_registries\n  }\n}\n\nfragment ContainerRegistryEditorModalFragment on ContainerRegistry {\n  id\n  hostname\n  config {\n    url\n    type\n    project\n    username\n    ssl_verify\n  }\n}\n"}}}();r.hash="0f4709e3ecfb8366c214098902385269";const i=r},9016:(e,n,a)=>{a.d(n,{b:()=>l});var r=a(77678);const i={"Cannot read property 'map' of null":"error.APINotSupported","TypeError: NetworkError when attempting to fetch resource.":"error.NetworkConnectionFailed","Login failed. Check login information.":"error.LoginFailed","User credential mismatch.":"error.LoginFailed","Authentication failed. Check information and manager status.":"error.AuthenticationFailed","Too many failed login attempts":"error.TooManyLoginFailures","server responded failure: 400 Bad Request - The virtual folder already exists with the same name.":"error.VirtualFolderAlreadyExist","400 Bad Request - The virtual folder already exists with the same name.":"error.VirtualFolderAlreadyExist","server responded failure: 400 Bad Request - One of your accessible vfolders already has the name you requested.":"error.VirtualFolderAlreadyExist","server responded failure: 400 Bad Request - You cannot create more vfolders.":"error.MaximumVfolderCreation","server responded failure: 400 Bad Request - Missing or invalid API parameters. (You cannot create more vfolders.)":"error.MaximumVfolderCreation","server responded failure: 400 Bad Request - Cannot change the options of a vfolder that is not owned by myself.":"error.CannotChangeVirtualFolderOption","server responded failure: 403 Forbidden - Cannot share private dot-prefixed vfolders.":"error.CannotSharePrivateAutomountFolder","server responded failure: 404 Not Found - No such vfolder invitation.":"error.FolderSharingNotAvailableToUser","server responded failure: 404 Not Found - No such user.":"error.FolderSharingNotAvailableToUser","server responded failure: 412 Precondition Failed - You have reached your resource limit.":"error.ReachedResourceLimit","Cannot read property 'split' of undefined":"error.UserHasNoGroup"},t={"\\w*not found matched token with email\\w*":"error.InvalidSignupToken","\\w*Access key not found\\w*":"error.LoginInformationMismatch","\\w*401 Unauthorized - Credential/signature mismatch\\w*":"error.LoginInformationMismatch",'integrity error: duplicate key value violates unique constraint "pk_resource_presets"[\\n]DETAIL:  Key \\(name\\)=\\([\\w]+\\) already exists.[\\n]':"error.ResourcePolicyAlreadyExist",'integrity error: duplicate key value violates unique constraint "pk_scaling_groups"[\\n]DETAIL:  Key \\(name\\)=\\([\\w]+\\) already exists.[\\n]':"error.ScalingGroupAlreadyExist",'integrity error: duplicate key value violates unique constraint "uq_users_username"[\\n]DETAIL:  Key \\(username\\)=\\([\\w]+\\) already exists.[\\n]':"error.UserNameAlreadyExist","server responded failure: 400 Bad Request - Missing or invalid API parameters. (Your resource quota is exceeded. (cpu=24 mem=512g cuda.shares=80))":"error.ResourceLimitExceed",'\\w*Key \\(name\\)=\\(.+\\) is still referenced from table "keypairs"\\.\\w*':"error.ResourcePolicyStillReferenced","Your resource request is smaller than the minimum required by the image. (\\w*)":"error.SmallerResourceThenImageRequires"},l=()=>{const{t:e}=(0,r.Bd)();return{relieve:n=>{if("undefined"===typeof n)return void 0===globalThis.backendaiclient||null===globalThis.backendaiclient?"_DISCONNECTED":"Problem occurred.";if(!0===globalThis.backendaiwebui.debug)return n;if({}.hasOwnProperty.call(i,n))return e(i[n]);for(const a of Object.keys(t))if(RegExp(a).test(n))return e(t[a]);return n}}}},69048:(e,n,a)=>{a.d(n,{A:()=>o});var r=a(58168),i=a(76998);const t={icon:{tag:"svg",attrs:{viewBox:"64 64 896 896",focusable:"false"},children:[{tag:"path",attrs:{d:"M168 504.2c1-43.7 10-86.1 26.9-126 17.3-41 42.1-77.7 73.7-109.4S337 212.3 378 195c42.4-17.9 87.4-27 133.9-27s91.5 9.1 133.8 27A341.5 341.5 0 01755 268.8c9.9 9.9 19.2 20.4 27.8 31.4l-60.2 47a8 8 0 003 14.1l175.7 43c5 1.2 9.9-2.6 9.9-7.7l.8-180.9c0-6.7-7.7-10.5-12.9-6.3l-56.4 44.1C765.8 155.1 646.2 92 511.8 92 282.7 92 96.3 275.6 92 503.8a8 8 0 008 8.2h60c4.4 0 7.9-3.5 8-7.8zm756 7.8h-60c-4.4 0-7.9 3.5-8 7.8-1 43.7-10 86.1-26.9 126-17.3 41-42.1 77.8-73.7 109.4A342.45 342.45 0 01512.1 856a342.24 342.24 0 01-243.2-100.8c-9.9-9.9-19.2-20.4-27.8-31.4l60.2-47a8 8 0 00-3-14.1l-175.7-43c-5-1.2-9.9 2.6-9.9 7.7l-.7 181c0 6.7 7.7 10.5 12.9 6.3l56.4-44.1C258.2 868.9 377.8 932 512.2 932c229.2 0 415.5-183.7 419.8-411.8a8 8 0 00-8-8.2z"}}]},name:"sync",theme:"outlined"};var l=a(13410),s=function(e,n){return i.createElement(l.A,(0,r.A)({},e,{ref:n,icon:t}))};const o=i.forwardRef(s)}}]);
//# sourceMappingURL=8247.fe5f604e.chunk.js.map