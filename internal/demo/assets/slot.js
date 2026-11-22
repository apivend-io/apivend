// The machine's coin slot on the demo page: the apivend mark in 3D (from the
// brand SVG's paths). For every settled payment a silver coin goes into the
// mark's gold slot, the slot drops away with a check in its place, and a fresh
// slot falls in (steps 4 and 5 of apivend.io's story). three.js 0.170.0 is served from this binary (THREE-LICENSE.txt).
// Without WebGL, or with reduced motion, the flat mark shows instead.

import * as THREE from 'three';
import { RoomEnvironment } from './RoomEnvironment.js';

const stage = document.getElementById('stage');
const MARK = '/assets/apivend-mark-on-dark.svg';
const params = new URLSearchParams(location.search);
const frozen = params.has('t');            // ?t=<seconds into a drop> renders one still frame
const reduce = matchMedia('(prefers-reduced-motion: reduce)').matches;

function fallback() {                      // no WebGL, or reduced motion: the flat mark
  stage.innerHTML = '<img src="' + MARK + '" alt="">';
  stage.classList.add('flat');               // the page then explains the slot in words
}
let renderer = null;
if (!reduce || frozen) {
  try { renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, preserveDrawingBuffer: frozen }); } catch { renderer = null; }
}
if (renderer) start(renderer); else fallback();

function start(renderer) {

renderer.setPixelRatio(Math.min(devicePixelRatio, 2));
renderer.toneMapping = THREE.ACESFilmicToneMapping;
renderer.toneMappingExposure = 1.0;
renderer.outputColorSpace = THREE.SRGBColorSpace;
stage.append(renderer.domElement);

const scene = new THREE.Scene();
const pmrem = new THREE.PMREMGenerator(renderer);
scene.environment = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
scene.environmentIntensity = 0.9;

const camera = new THREE.PerspectiveCamera(24, 1, 10, 2000);
camera.position.set(-46, 26, 380);
camera.lookAt(0, 4, 0);

// Brand geometry, from the mark's SVG (120 x 120 box, centre 60,60, y down).
// Brackets: 10-unit strokes with square caps, as the paths M40 22H22v76h18.
const S = (x, y) => new THREE.Vector2(x - 60, 60 - y);
function bracketShape(flip) {
  const f = x => flip ? 120 - x : x;
  const pts = [[17,17],[45,17],[45,27],[27,27],[27,93],[45,93],[45,103],[17,103]].map(([x, y]) => S(f(x), y));
  if (flip) pts.reverse();
  return new THREE.Shape(pts);
}
const extrude = { depth: 5, bevelEnabled: true, bevelThickness: 1.0, bevelSize: 0.8, bevelSegments: 4, curveSegments: 4 };
const paperMat = new THREE.MeshStandardMaterial({ color: 0xE9ECF0, roughness: 0.38, metalness: 0.55 });
for (const flip of [false, true]) {
  const g = new THREE.ExtrudeGeometry(bracketShape(flip), extrude);
  g.translate(0, 0, -2.5);
  scene.add(new THREE.Mesh(g, paperMat));
}

// The slot: the mark's gold disc with a reeded edge and the slit through it.
// (Called "coin" below, as the mark's middle has always been in this file.)
const coinMat = new THREE.MeshStandardMaterial({ color: 0xF2B03D, roughness: 0.3, metalness: 0.6, envMapIntensity: 1.3 });
const edgeMat = new THREE.MeshStandardMaterial({ color: 0xD9962A, roughness: 0.45, metalness: 0.85 });
const inkMat = new THREE.MeshStandardMaterial({ color: 0x0E1116, roughness: 0.7, metalness: 0.0 });
const plateMat = coinMat.clone();                  // its own, so it can flash
function makeCoin() {
  const coin = new THREE.Group();
  const face = new THREE.CylinderGeometry(19, 19, 5, 96, 1);
  face.rotateX(Math.PI / 2);
  coin.add(new THREE.Mesh(face, [edgeMat, plateMat, plateMat]));
  // Reeding: thin ridges around the edge.
  const ridge = new THREE.BoxGeometry(0.7, 0.5, 4.2);
  const ridges = new THREE.InstancedMesh(ridge, edgeMat, 90);
  const m = new THREE.Matrix4();
  for (let i = 0; i < 90; i++) {
    const a = i / 90 * Math.PI * 2;
    m.makeRotationZ(a).setPosition(Math.cos(a) * 19.1, Math.sin(a) * 19.1, 0);
    m.multiply(new THREE.Matrix4().makeRotationZ(Math.PI / 2));
    ridges.setMatrixAt(i, m);
  }
  coin.add(ridges);
  // The slot (8 x 26, radius 4), as a dark capsule through the coin.
  const cap = new THREE.Shape();
  cap.absarc(0, 9, 4, 0, Math.PI, false);
  cap.absarc(0, -9, 4, Math.PI, Math.PI * 2, false);
  const slot = new THREE.ExtrudeGeometry(cap, { depth: 5.4, bevelEnabled: false, curveSegments: 16 });
  slot.translate(0, 0, -2.7);
  coin.add(new THREE.Mesh(slot, inkMat));
  return coin;
}
const coin = makeCoin();
scene.add(coin);

// The payment: a small silver coin with a gold dollar sign, 17 across, so it
// fits the slit (26 tall, 8 wide) edge-on. The same coin as on apivend.io.
const silverMat = new THREE.MeshStandardMaterial({ color: 0xDCE0E6, roughness: 0.26, metalness: 0.92, envMapIntensity: 1.4 });
const silverEdge = new THREE.MeshStandardMaterial({ color: 0xA9AFB8, roughness: 0.4, metalness: 0.95 });
const silverParts = (() => {
  const face = new THREE.CylinderGeometry(8.5, 8.5, 2.2, 72, 1);
  face.rotateX(Math.PI / 2);
  const ridge = new THREE.BoxGeometry(0.6, 0.45, 1.4);
  const ring = new THREE.TorusGeometry(6.2, 0.4, 8, 64);
  const s = new THREE.CatmullRomCurve3([
    [2.0, 2.2], [0.6, 3.0], [-1.4, 2.6], [-2.0, 1.4], [-1.0, 0.4],
    [1.0, -0.4], [2.0, -1.4], [1.4, -2.6], [-0.6, -3.0], [-2.0, -2.2],
  ].map(([x, y]) => new THREE.Vector3(x, y, 0)));
  return { face, ridge, ring, curve: new THREE.TubeGeometry(s, 64, 0.55, 8, false), bar: new THREE.BoxGeometry(0.85, 9.6, 1.2) };
})();
function makeSilver() {
  const c = new THREE.Group();
  c.add(new THREE.Mesh(silverParts.face, [silverEdge, silverMat, silverMat]));
  const ridges = new THREE.InstancedMesh(silverParts.ridge, silverEdge, 56);
  const m = new THREE.Matrix4();
  for (let i = 0; i < 56; i++) {
    const a = i / 56 * Math.PI * 2;
    m.makeRotationZ(a).setPosition(Math.cos(a) * 8.6, Math.sin(a) * 8.6, 0);
    m.multiply(new THREE.Matrix4().makeRotationZ(Math.PI / 2));
    ridges.setMatrixAt(i, m);
  }
  c.add(ridges);
  for (const side of [1, -1]) {
    const r = new THREE.Mesh(silverParts.ring, silverEdge); r.position.z = side * 1.15; c.add(r);
    const d = new THREE.Group();
    d.add(new THREE.Mesh(silverParts.curve, coinMat), new THREE.Mesh(silverParts.bar, coinMat));
    d.scale.z = 0.6; d.position.z = side * 1.2;
    if (side < 0) d.rotation.y = Math.PI;
    c.add(d);
  }
  return c;
}
const payment = makeSilver();
payment.visible = false;
scene.add(payment);

// The check: gold, in the slot's place once the slot has dropped (served).
const checkMat = coinMat.clone();
const tick = new THREE.Group();
for (const [len, ang, x, y] of [[17, -Math.PI / 4, -8.5, -3], [33, Math.PI / 3.6, 4.5, 3.5]]) {
  const arm = new THREE.Mesh(new THREE.BoxGeometry(len, 7, 5), checkMat);
  arm.rotation.z = ang; arm.position.set(x, y, 0);
  tick.add(arm);
}
tick.visible = false;
scene.add(tick);

// A soft gold glow behind the check, and a back light on the brackets.
const halo = new THREE.Sprite(new THREE.SpriteMaterial({ map: (() => {
  const c = document.createElement('canvas'); c.width = c.height = 128;
  const x = c.getContext('2d'), gr = x.createRadialGradient(64, 64, 0, 64, 64, 64);
  gr.addColorStop(0, 'rgba(255,214,130,0.95)'); gr.addColorStop(0.35, 'rgba(242,176,61,0.45)'); gr.addColorStop(1, 'rgba(242,176,61,0)');
  x.fillStyle = gr; x.fillRect(0, 0, 128, 128);
  const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; return t;
})(), transparent: true, depthWrite: false, opacity: 0 }));
halo.scale.set(110, 110, 1); halo.position.z = -12;
scene.add(halo);
const back = new THREE.PointLight(0xF2B03D, 0, 160, 0); back.position.set(0, 0, -20);
scene.add(back);
const glow = new THREE.Color(0xF2B03D);

const key = new THREE.DirectionalLight(0xffffff, 1.6); key.position.set(-120, 140, 200); scene.add(key);
const rim = new THREE.DirectionalLight(0xffe2b0, 0.8); rim.position.set(120, -40, -60); scene.add(rim);

function resize() {
  const { width, height } = stage.getBoundingClientRect();
  renderer.setSize(width, height, false);
  camera.aspect = width / height; camera.updateProjectionMatrix();
}
resize(); addEventListener('resize', resize);

// Timeline of one payment, in seconds after "pay":
//   0.00-LAND  a silver coin drops in above the mark and bounces, while the
//              slot turns face-on to take it; both come to rest together
//   INS-+0.9   the coin comes forward, turns edge-on and slides into the slit
//              (its receipt prints at PRINT, as it goes in)
//   DROP-      the slot flashes, turns edge-on and drops; the check pops up
//   newAt-     a fresh slot falls in under gravity and bounces twice, the
//              check shrinks away; its extra spin fades into the idle spin
const RATE = 1.1;                                   // idle spin, rad/s
const clamp = (v, a = 0, b = 1) => Math.min(b, Math.max(a, v));
const easeIn = t => t * t * t, easeInOut = t => t < .5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
const easeOutCubic = t => 1 - Math.pow(1 - t, 3);

// Height of a coin dropped from H with gravity G that bounces with restitution E.
const H = 150, G = 1500, E = 0.32;
const T0 = Math.sqrt(2 * H / G);                    // time to first impact
const bounces = (() => { const out = []; let v = G * T0 * E, t = T0;
  for (let i = 0; i < 2; i++) { const d = 2 * v / G; out.push([t, v, d]); t += d; v *= E; } return { list: out, end: t }; })();
function fallY(t) {
  if (t < T0) return H - 0.5 * G * t * t;
  for (const [t0, v, d] of bounces.list) if (t < t0 + d) { const u = t - t0; return v * u - 0.5 * G * u * u; }
  return 0;
}

const LAND = bounces.end, INS = LAND + 0.1, PRINT = INS + 0.75, DROP = INS + 0.9;
const NEW_AT = DROP + 1.8;
const HOVER_Y = 55;                                 // just above the mark, over the brackets' gap
const lerp = (a, b, t) => a + (b - a) * t;
const easeOutBack = t => t <= 0 ? 0 : 1 + 2.7 * Math.pow(t - 1, 3) + 1.7 * Math.pow(t - 1, 2);
let glowLevel = 0;
// The main coin's pose. newAt is when the fresh coin starts falling in: after
// the one coin of a single payment, or after the last coin of a pour.
function pose(clock, drop, spinAtDrop, newAt) {
  // clock: seconds since the page started; drop: seconds into a payment or null
  const bob = Math.sin(clock * 1.3) * 1.2;
  let y = bob, spin = clock * RATE;
  const face = Math.round(spinAtDrop / Math.PI) * Math.PI;
  if (drop != null) {
    if (drop < DROP) spin = spinAtDrop + (face - spinAtDrop) * easeInOut(clamp(drop / LAND));
    else if (drop < newAt) {
      const t = drop - DROP, edge = face + Math.PI / 2;
      if (t < 0.35) spin = face + (edge - face) * easeInOut(t / 0.35);
      else { spin = edge; y = -easeIn(clamp((t - 0.35) / 0.6)) * 190; }
    } else {
      const t = drop - newAt;
      spin = clock * RATE + 2 * Math.PI * (1 - easeOutCubic(clamp(t / 1.6)));
      y = fallY(t) + bob * clamp((t - bounces.end) / 0.6);
    }
  }
  coin.position.y = y; coin.rotation.y = spin;

  // The silver coin.
  payment.visible = drop != null && drop < DROP;
  if (payment.visible) {
    payment.rotation.y = 0;
    if (drop < LAND) payment.position.set(0, HOVER_Y + fallY(drop), 0);
    else if (drop < INS) payment.position.set(0, HOVER_Y + Math.sin(clock * 1.3) * 1.2 * clamp((drop - LAND) / 0.6), 0);
    else {
      const t = drop - INS;
      payment.rotation.y = Math.PI / 2 * easeInOut(clamp(t / 0.45));
      payment.position.set(0, lerp(HOVER_Y, bob, easeInOut(clamp((t - 0.1) / 0.4))), lerp(0, 22, easeOutCubic(clamp(t / 0.3))));
      if (t > 0.5) payment.position.z = lerp(22, -22, easeIn(clamp((t - 0.5) / 0.4)));
    }
  }

  // The slot flashes as it takes the coin; the check pops up in its place.
  const took = drop != null && drop >= DROP ? Math.max(0, 1 - (drop - DROP) / 0.35) : 0;
  plateMat.emissive.copy(glow).multiplyScalar(0.5 * took);
  let cs = 0;
  if (drop != null && drop >= DROP) cs = drop < newAt ? easeOutBack(clamp((drop - DROP - 0.85) / 0.45)) : 1 - clamp((drop - newAt) / 0.25);
  tick.visible = cs > 0.001; tick.scale.setScalar(Math.max(cs, 0.001));
  tick.position.y = bob; tick.rotation.y = Math.sin(clock * 1.2) * 0.35;
  checkMat.emissive.copy(glow).multiplyScalar(0.12 + 0.06 * Math.sin(clock * 3));
  glowLevel += ((cs > 0.5 ? 0.4 : took * 0.6) - glowLevel) * 0.08;
  const pulse = 0.85 + 0.15 * Math.sin(clock * 4);
  halo.material.opacity = glowLevel * pulse;
  back.intensity = glowLevel * pulse * 2.2;
}

// Followers: the extra coins of a burst, tumbling through from above. At most
// POOL are ever on screen, however many payments arrived.
const POOL = 12, BELOW = -96;                       // BELOW: inside the slot
const pool = Array.from({ length: POOL }, () => { const c = makeSilver(); c.visible = false; scene.add(c); return c; });
function follower(c, t, f) {
  // t: seconds since this coin started; f: its random seed
  if (t < 0) { c.visible = false; return; }
  c.visible = true;
  c.position.set((f - 0.5) * 16, 150 + f * 30 - 0.5 * G * t * t, -4 + f * 8);
  c.rotation.set(t * (4 + f * 6), t * (3 + f * 5), (f - 0.5) * 0.6);
}

// The tag: what the silver coin is, following it until it enters the slot.
const tag = document.createElement('div');
tag.className = 'coin-tag'; tag.hidden = true; stage.append(tag);
const v = new THREE.Vector3();
function placeTag(alpha) {
  // Beside the coin, on its right, kept inside the stage on narrow screens.
  v.copy(payment.position).project(camera);
  const r = stage.getBoundingClientRect(), w = tag.offsetWidth, h = tag.offsetHeight;
  const x = Math.min(Math.max((v.x + 1) / 2 * r.width + 22, 4), r.width - w - 4);
  const y = Math.min(Math.max((1 - v.y) / 2 * r.height - h / 2, 4), r.height - h - 4);
  tag.style.transform = `translate(${x}px, ${y}px)`;
  tag.style.opacity = alpha;
}

if (frozen) {
  const t = parseFloat(params.get('t')) || 0;
  pose(0.8 + Math.max(0, t), t < 0 ? null : t, 0.8 * RATE, NEW_AT);
  renderer.render(scene, camera);
} else {
  const t0 = performance.now();
  let run = null, visible = true;          // run: the animation in progress
  new IntersectionObserver(([e]) => { visible = e.isIntersecting; }).observe(stage);
  const watching = () => visible && !document.hidden;

  // settle(payments, onPrint, label): called once per check of the feed,
  // with the payments that settled since the last one (newest first) and a
  // callback that prints receipts. One coin per check, however many
  // settled: its tag says how many, and their receipts print as it enters
  // the slot. Payments that arrive during a run print at once and raise the
  // count on the tag; they never add coins (several coins at once read as
  // noise, not as payments). When nobody is watching, receipts print at
  // once.
  const tagText = (n, label) => n === 1 && label ? label : n.toLocaleString('en-US') + ' payments settled';
  window.apivendSlot = {
    settle(payments, onPrint, label) {
      if (!payments.length) return;
      if (!watching()) { onPrint(payments); return; }
      const now = performance.now();
      if (run) {
        onPrint(payments);
        run.count += payments.length;
        tag.textContent = tagText(run.count, null);
        return;
      }
      run = {
        at: now, spinAtDrop: (now - t0) / 1000 * RATE, followers: [], onPrint,
        first: payments, printed: false, newAt: NEW_AT, count: payments.length,
        label: tagText(payments.length, label),
      };
      tag.textContent = run.label; tag.hidden = false;
      // Receipts never wait on the animation: if frames stop (a throttled
      // tab that still counts as visible), everything prints after 8 s.
      const mine = run;
      setTimeout(() => { if (run === mine) finish(); }, 8000);
    },
  };

  const finish = () => {
    if (!run) return;
    if (!run.printed) run.onPrint(run.first);
    for (const f of run.followers) if (f.payments.length) run.onPrint(f.payments);
    for (const c of pool) c.visible = false;
    tag.hidden = true; run = null;
  };
  renderer.setAnimationLoop(now => {
    if (!watching()) { finish(); return; }
    let d = null;
    if (run) {
      d = (now - run.at) / 1000;
      if (d > PRINT && !run.printed) { run.printed = true; run.onPrint(run.first); }
      run.followers.forEach((f, i) => {
        follower(pool[i], d - f.start, f.seed);
        if (f.payments.length && pool[i].position.y < BELOW) { run.onPrint(f.payments); f.payments = []; }
      });
      if (run.label) placeTag(d < 0.1 ? d / 0.1 : d > PRINT - 0.2 ? clamp((PRINT - d) / 0.2) : 1);
      if (d > run.newAt + bounces.end + 0.9) { finish(); d = null; }
    }
    pose((now - t0) / 1000, d, run ? run.spinAtDrop : 0, run ? run.newAt : NEW_AT);
    renderer.render(scene, camera);
  });
}
}
